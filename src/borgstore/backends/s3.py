try:
    import boto3
except ImportError:
    boto3 = None

import re
from typing import Iterator, Optional

from ..constants import TMP_SUFFIX

from ._base import BackendBase, ItemInfo, validate_name
from .errors import BackendError, BackendMustBeOpen, BackendMustNotBeOpen, BackendDoesNotExist, BackendAlreadyExists
from .errors import ObjectNotFound
import concurrent.futures
import threading
import os
import time
import sys
import logging


MAX_WORKERS = int(os.environ.get("BORG_S3_CONCURRENT_UPLOADS", "16"))
async_executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)

def get_s3_backend(url):
    if boto3 is None:
        return None

    # s3:[profile|(access_key_id:access_key_secret)@][schema://hostname[:port]]/bucket/path
    s3_regex = r"""
        s3:
        ((
            (?P<profile>[^@:]+)  # profile (no colons allowed)
            |
            (?P<access_key_id>[^:@]+):(?P<access_key_secret>[^@]+)  # access key and secret
        )@)?  # optional authentication
        (?P<schema>[^:/]+)://
        (?P<hostname>[^:/]+)
        (:(?P<port>\d+))?/
        (?P<bucket>[^/]+)/  # bucket name
        (?P<path>.+)  # path
    """
    m = re.match(s3_regex, url, re.VERBOSE)
    if m:
        profile = m["profile"]
        access_key_id = m["access_key_id"]
        access_key_secret = m["access_key_secret"]
        if profile is not None and access_key_id is not None:
            raise BackendError("S3: profile and access_key_id cannot be specified at the same time")
        if access_key_id is not None and access_key_secret is None:
            raise BackendError("S3: access_key_secret is mandatory when access_key_id is specified")
        schema = m["schema"]
        hostname = m["hostname"]
        port = m["port"]
        bucket = m["bucket"]
        path = m["path"]

        endpoint_url = None
        if schema and hostname:
            endpoint_url = f"{schema}://{hostname}"
            if port:
                endpoint_url += f":{port}"
        return S3(bucket=bucket, path=path, profile=profile,
                  access_key_id=access_key_id, access_key_secret=access_key_secret, endpoint_url=endpoint_url)


class Queue:
    def __init__(self, max_readers, max_writers):
        self.reader_count = 0
        self.writer_count = 0
        self.max_readers = max_readers
        self.max_writers = max_writers
        self.read_semaphore = threading.Semaphore(max_readers)
        self.write_semaphore = threading.Semaphore(max_writers)
        self.reader_lock = threading.Lock()
        self.writer_lock = threading.Lock()

    def acquire(self):
        class ReadContext:
            def __init__(self, semaphore):
                self.semaphore = semaphore

            def __enter__(self):
                self.semaphore._acquire()

            def __exit__(self, exc_type, exc_value, traceback):
                self.semaphore._release()

        return ReadContext(self)

    def debug(self, msg, param=None):
        #print(msg, param, "reader count: " + str(self.reader_count), "writer count: " + str(self.writer_count))
        pass

    def _acquire(self):
        self.debug("begin _acquire")
        with self.reader_lock:
            self.reader_count += 1
            for i in range(self.max_readers):
                self.read_semaphore.acquire()
        with self.writer_lock:
            self.writer_count += 1
            for i in range(self.max_writers):
                self.write_semaphore.acquire()
        self.debug("done _acquire")

    def _release(self):
        self.debug("begin _release")
        with self.reader_lock:
            self.reader_count -= 1
            for i in range(self.max_readers):
                self.read_semaphore.release()
        with self.writer_lock:
            self.writer_count -= 1
            for i in range(self.max_writers):
                self.write_semaphore.release()
        self.debug("done _release")

    def acquire_read(self):
        class ReadContext:
            def __init__(self, semaphore):
                self.semaphore = semaphore

            def __enter__(self):
                self.semaphore._acquire_read()

            def __exit__(self, exc_type, exc_value, traceback):
                self.semaphore._release_read()

        return ReadContext(self)

    def _acquire_read(self):
        self.debug("begin _acquire_read")
        self.read_semaphore.acquire()
        with self.reader_lock:
            self.reader_count += 1
            if self.reader_count == 1:
                for i in range(self.max_writers):
                    self.write_semaphore.acquire()
            self.debug("done _acquire_read")

    def _release_read(self):
        self.debug("begin _release_read")
        with self.reader_lock:
            self.reader_count -= 1
            if self.reader_count == 0:
                for i in range(self.max_writers):
                    self.write_semaphore.release()
            self.debug("done _release_read")

        self.read_semaphore.release()

    def acquire_write(self):
        class WriteContext:
            def __init__(self, semaphore):
                self.semaphore = semaphore

            def __enter__(self):
                self.semaphore._acquire_write()

            def __exit__(self, exc_type, exc_value, traceback):
                self.semaphore._release_write()

        return WriteContext(self)

    def _acquire_write(self):
        self.debug("begin _acquire_write")
        self.write_semaphore.acquire()
        with self.writer_lock:
            self.writer_count += 1
            if self.writer_count == 1:
                for i in range(self.max_readers):
                    self.read_semaphore.acquire()
            self.debug("done _acquire_write")

    def _release_write(self):
        self.debug("begin _release_write")
        with self.writer_lock:
            self.writer_count -= 1
            if self.writer_count == 0:
                for i in range(self.max_readers):
                    self.read_semaphore.release()
            self.debug("done _release_write")
        self.write_semaphore.release()


class S3(BackendBase):
    def __init__(self, bucket: str, path: str, profile: Optional[str] = None,
                 access_key_id: Optional[str] = None, access_key_secret: Optional[str] = None,
                 endpoint_url: Optional[str] = None):
        self.delimiter = '/'
        self.dir_file = '.dir'

        logging.getLogger("urllib3").setLevel(logging.ERROR)

        self.bucket = bucket
        self.base_path = path.rstrip(self.delimiter) + self.delimiter  # Ensure it ends with '/'
        self.opened = False
        if profile:
            session = boto3.Session(profile_name=profile)
        elif access_key_id and access_key_secret:
            session = boto3.Session(aws_access_key_id=access_key_id, aws_secret_access_key=access_key_secret)
        else:
            session = boto3.Session()
        self.s3 = session.client("s3", endpoint_url=endpoint_url)
        self.queue = Queue(MAX_WORKERS, MAX_WORKERS)

    def preload(self, iter: Iterator[str]) -> None:
        """preload values"""
        pass

    def _mkdir(self, name):
        try:
            key = (self.base_path + name).rstrip(self.delimiter) + self.delimiter + self.dir_file
            self.s3.put_object(Bucket=self.bucket, Key=key)
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def create(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        try:
            with self.queue.acquire():
                objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.base_path,
                                                  Delimiter=self.delimiter, MaxKeys=1)
                if objects["KeyCount"] > 0:
                    raise BackendAlreadyExists(f"Backend already exists: {self.base_path}")
                self._mkdir("")
        except self.s3.exceptions.NoSuchBucket:
            raise BackendDoesNotExist(f"S3 bucket does not exist: {self.bucket}")
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def destroy(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        try:
            with self.queue.acquire():
                objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.base_path,
                                                  Delimiter=self.delimiter, MaxKeys=1)
                if objects["KeyCount"] == 0:
                    raise BackendDoesNotExist(f"Backend does not exist: {self.base_path}")
                is_truncated = True
                while is_truncated:
                    objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.base_path, MaxKeys=1000)
                    is_truncated = objects['IsTruncated']
                    if "Contents" in objects:
                        self.s3.delete_objects(
                            Bucket=self.bucket,
                            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects["Contents"]]}
                        )
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def open(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        self.opened = True

    def close(self):
        if not self.opened:
            raise BackendMustBeOpen()
        with self.queue.acquire():
            self.opened = False

    def _async_store(self, key, value, lock):
        try:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=value)
        except self.s3.exceptions.ClientError:
            pass
        lock.__exit__(None, None, None)

    def store(self, name, value):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        lock = self.queue.acquire_write()
        lock.__enter__()
        async_executor.submit(lambda : self._async_store(key, value, lock))

    def load(self, name, *, size=None, offset=0):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        with self.queue.acquire_read():
            try:
                if size is None and offset == 0:
                    obj = self.s3.get_object(Bucket=self.bucket, Key=key)
                    return obj["Body"].read()
                elif size is not None and offset == 0:
                    obj = self.s3.get_object(Bucket=self.bucket, Key=key, Range=f"bytes=0-{size - 1}")
                    return obj["Body"].read()
                elif size is None and offset != 0:
                    head = self.s3.head_object(Bucket=self.bucket, Key=key)
                    length = head["ContentLength"]
                    obj = self.s3.get_object(Bucket=self.bucket, Key=key, Range=f"bytes={offset}-{length - 1}")
                    return obj["Body"].read()
                elif size is not None and offset != 0:
                    obj = self.s3.get_object(Bucket=self.bucket, Key=key, Range=f"bytes={offset}-{offset + size - 1}")
                    return obj["Body"].read()
            except self.s3.exceptions.NoSuchKey:
                raise ObjectNotFound(name)

    def _async_delete(self, key, lock):
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
        except self.s3.exceptions.ClientError:
            pass
        lock.__exit__(None, None, None)

    def delete(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        if os.environ.get('BORGSTORE_TEST_S3_URL') is None:
            lock = self.queue.acquire_write()
            lock.__enter__()
            async_executor.submit(lambda : self._async_delete(key, lock))
        else:
            with self.queue.acquire_write():
                try:
                    self.s3.head_object(Bucket=self.bucket, Key=key)
                    self.s3.delete_object(Bucket=self.bucket, Key=key)
                except self.s3.exceptions.NoSuchKey:
                    raise ObjectNotFound(name)
                except self.s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        raise ObjectNotFound(name)

    def move(self, curr_name, new_name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(curr_name)
        validate_name(new_name)
        src_key = self.base_path + curr_name
        dest_key = self.base_path + new_name
        with self.queue.acquire():
            try:
                self.s3.copy_object(Bucket=self.bucket, CopySource={"Bucket": self.bucket, "Key": src_key},
                                    Key=dest_key)
                self.s3.delete_object(Bucket=self.bucket, Key=src_key)
            except self.s3.exceptions.NoSuchKey:
                raise ObjectNotFound(curr_name)

    def list(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        base_prefix = (self.base_path + name).rstrip(self.delimiter) + self.delimiter
        try:
            start_after = ''
            is_truncated = True
            while is_truncated:
                with self.queue.acquire_read():
                    objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=base_prefix,
                                                      Delimiter=self.delimiter, MaxKeys=1000, StartAfter=start_after)
                if objects['KeyCount'] == 0:
                    raise ObjectNotFound(name)
                is_truncated = objects["IsTruncated"]
                if "Contents" not in objects and "CommonPrefixes" not in objects:
                    pass
                for obj in objects.get("Contents", []):
                    obj_name = obj["Key"][len(base_prefix):]  # Remove base_path prefix
                    if obj_name == self.dir_file:
                        continue
                    if obj_name.endswith(TMP_SUFFIX):
                        continue
                    start_after = obj["Key"]
                    yield ItemInfo(name=obj_name, exists=True, size=obj["Size"], directory=False)
                for prefix in objects.get("CommonPrefixes", []):
                    dir_name = prefix["Prefix"][len(base_prefix):-1]  # Remove base_path prefix and trailing slash
                    yield ItemInfo(name=dir_name, exists=True, size=0, directory=True)
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def mkdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        with self.queue.acquire_write():
            self._mkdir(name)

    def rmdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        with self.queue.acquire_write():
            prefix = self.base_path + name.rstrip(self.delimiter) + self.delimiter
            objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, Delimiter=self.delimiter, MaxKeys=2)
            if "Contents" in objects and len(objects["Contents"]) > 1:
                raise BackendError(f"Directory not empty: {name}")
            self.s3.delete_object(Bucket=self.bucket, Key=prefix + self.dir_file)

    def info(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        with self.queue.acquire_read():
            try:
                obj = self.s3.head_object(Bucket=self.bucket, Key=key)
                return ItemInfo(name=name, exists=True, directory=False, size=obj["ContentLength"])
            except self.s3.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    try:
                        self.s3.head_object(Bucket=self.bucket, Key=key + self.delimiter + self.dir_file)
                        return ItemInfo(name=name, exists=True, directory=True, size=0)
                    except self.s3.exceptions.ClientError:
                        pass
                    return ItemInfo(name=name, exists=False, directory=False, size=0)
                raise BackendError(f"S3 error: {e}")
