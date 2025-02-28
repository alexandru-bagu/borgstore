
import concurrent
import concurrent.futures
import os

class Parallelization:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        self.workers = int(os.environ.get("BORG_PARALLEL_WORKERS", "16"))
        self.preload_cache_size = int(os.environ.get("BORG_PRELOAD_CACHE_SIZE", "64"))
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.workers)
