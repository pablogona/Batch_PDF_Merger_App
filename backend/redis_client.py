# backend/redis_client.py

import threading
import time
import json
from collections import defaultdict

class InMemoryRedis:
    def __init__(self, cleanup_interval=300):
        self._data = defaultdict(dict)
        self._lock = threading.Lock()
        threading.Thread(target=self._cleanup, daemon=True).start()
        self.cleanup_interval = cleanup_interval

    def set(self, key, value):
        with self._lock:
            self._data[key] = (value, time.time())

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
            if item:
                return item[0]
            return None

    def incr(self, key):
        with self._lock:
            current = self.get(key)
            if current is None:
                current = 0
            new_value = current + 1
            self.set(key, new_value)
            return new_value

    def _cleanup(self):
        while True:
            time.sleep(self.cleanup_interval)
            self._perform_cleanup()

    def _perform_cleanup(self):
        current_time = time.time()
        with self._lock:
            for key in list(self._data.keys()):
                if current_time - self._data[key][1] > 3600:  # 1 hour
                    del self._data[key]

redis_client = InMemoryRedis()