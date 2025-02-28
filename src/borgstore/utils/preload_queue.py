import collections
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Any, Callable, List

class PreloadQueue:
    def __init__(self, item_list: List[Any], max_cache_size: int, download_function: Callable[[Any], Any], async_executor: ThreadPoolExecutor):
        self.item_list = item_list  # Iterator over the list
        self.item_set = set(item_list)  # Iterator over the list
        self.max_cache_size = max_cache_size
        self.download_function = download_function  # Function to download items
        self.async_executor = async_executor  # ThreadPoolExecutor for parallel processing
        
        self.cache = {}  # Dictionary to store items
        self.usage_count = collections.Counter()  # Track usage of items
        self.count = 0
        self.lock = threading.Lock()
        self.not_empty = threading.Condition(self.lock)
        
        self._start_writer()
    
    def _start_writer(self):
        """Starts the writer tasks in the ThreadPoolExecutor."""
        current_item = None
        count = 0
        for i in range(0, len(self.item_list), 1):
            item = self.item_list[i]
            if current_item != item and count > 0:
                self.async_executor.submit(self._process_item, [current_item, count])
                current_item = item
                count = 0
            count = count + 1
        self.async_executor.submit(self._process_item, [current_item, count])
    
    def _count_item(self, item, data, increment):
        if item not in self.cache:
            self.usage_count[item] = 0
        self.usage_count[item] += increment
        
        if item in self.cache:
            if self.usage_count[item] == 0:
                del self.cache[item]
                if increment < 0:
                    self.count = self.count - 1
        else:
            if data is not None:
                self.cache[item] = data
                if increment > 0:
                    self.count = self.count + 1
        self.not_empty.notify_all()
    
    def _process_item(self, array):
        """Processes an individual item, adding it to the cache."""
        item = array[0]
        count = array[1]

        data = None
        with self.lock:
             if item in self.cache:
                data = self.cache[item]
        with self.lock:
            while self.count >= self.max_cache_size:
                self.not_empty.wait()
            self.count = self.count + 1
        if data is None:
            data = self.download_function(item)
        with self.lock:
            self.count = self.count - 1
            self._count_item(item, data, count)

    def get(self, item):
        """Fetch an item from the queue, ensuring ordered processing."""
        if item not in self.item_set:
            return self.download_function(item)
        with self.lock:
            if item in self.cache:
                data = self.cache[item]
                self._count_item(item, None, -1)
                return data
        data = self.download_function(item)
        with self.lock:
            self._count_item(item, data, -1)
        return data