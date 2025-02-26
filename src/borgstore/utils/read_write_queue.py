import threading

class ReadWriteQueue:
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