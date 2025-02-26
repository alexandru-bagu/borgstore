
import collections
import threading

class PreloadQueue:
    def __init__(self, item_list, max_cache_size, download_function, async_executor):
        self.item_list = iter(item_list)  # Iterator over the list
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
        for item in self.item_list:
            self.async_executor.submit(self._process_item, item)
    
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
    
    def _process_item(self, item):
        """Processes an individual item, adding it to the cache."""
        data = None
        
        with self.lock:
             if item in self.cache:
                data = self.cache[item]
        if data is None:
            data = self.download_function(item)
                
        with self.lock:
            while self.count >= self.max_cache_size:
                self.not_empty.wait()
            self._count_item(item, data, 1)
    
    def get(self, item):
        """Fetch an item from the queue, ensuring ordered processing."""
        with self.lock:
            if item in self.cache:
                data = self.cache[item]
                self._count_item(item, None, -1)
                return data
        data = self.download_function(item)
        with self.lock:
            self._count_item(item, data, -1)
        return data