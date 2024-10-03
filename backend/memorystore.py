import threading
import time
from collections import defaultdict

class MemoryStore:
    def __init__(self, cleanup_interval=300):
        self._data = defaultdict(dict)
        self._lock = threading.Lock()
        threading.Thread(target=self._cleanup, daemon=True).start()
        self.cleanup_interval = cleanup_interval

    def set(self, namespace, key, value):
        with self._lock:
            self._data[namespace][key] = (value, time.time())

    def get(self, namespace, key):
        with self._lock:
            item = self._data[namespace].get(key)
            if item:
                return item[0]
            return None

    def incr(self, namespace, key):
        with self._lock:
            current = self.get(namespace, key) or 0
            new_value = current + 1
            self.set(namespace, key, new_value)
            return new_value

    def _cleanup(self):
        while True:
            time.sleep(self.cleanup_interval)
            self._perform_cleanup()

    def _perform_cleanup(self):
        current_time = time.time()
        with self._lock:
            for namespace in list(self._data.keys()):
                for key in list(self._data[namespace].keys()):
                    if current_time - self._data[namespace][key][1] > 3600:  # 1 hour
                        del self._data[namespace][key]
                if not self._data[namespace]:
                    del self._data[namespace]

memory_store = MemoryStore()