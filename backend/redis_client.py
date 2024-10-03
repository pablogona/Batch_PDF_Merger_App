# backend/redis_client.py

import threading
import time
from collections import defaultdict

class InMemoryRedis:
    def __init__(self, cleanup_interval=300):
        """
        Initialize the in-memory Redis-like store.
        
        :param cleanup_interval: Interval in seconds to perform automatic cleanup of expired keys.
        """
        self._data = defaultdict(dict)  # Stores key-value pairs along with their timestamps.
        self._expirations = {}  # Tracks expiration times for keys.
        self._lock = threading.Lock()  # Ensures thread safety.
        self.cleanup_interval = cleanup_interval  # Interval to run the cleanup process.
        
        # Start a background thread to periodically clean up expired keys.
        threading.Thread(target=self._cleanup, daemon=True).start()

    def set(self, key, value, ex=None):
        """
        Set a value in the in-memory store.
        
        :param key: The key to set.
        :param value: The value to store.
        :param ex: Expiration time in seconds (optional).
        """
        with self._lock:
            self._data[key] = (value, time.time())
            if ex is not None:
                self._expirations[key] = time.time() + ex
            elif key in self._expirations:
                del self._expirations[key]

    def get(self, key):
        """
        Get a value from the in-memory store.
        
        :param key: The key to retrieve.
        :return: The value or None if not found or expired.
        """
        with self._lock:
            if key in self._expirations and time.time() > self._expirations[key]:
                # Key has expired, remove it.
                del self._data[key]
                del self._expirations[key]
                return None

            item = self._data.get(key)
            if item:
                return item[0]
            return None

    def incr(self, key):
        """
        Increment an integer value stored in the in-memory store.
        
        :param key: The key to increment.
        :return: The new value after incrementing.
        """
        with self._lock:
            current = self.get(key)
            if current is None:
                current = 0
            new_value = int(current) + 1
            self.set(key, new_value)
            return new_value

    def delete(self, key):
        """
        Delete a key from the in-memory store.
        
        :param key: The key to delete.
        """
        with self._lock:
            if key in self._data:
                del self._data[key]
            if key in self._expirations:
                del self._expirations[key]

    def _cleanup(self):
        """
        Periodically clean up expired keys based on their expiration times.
        This method runs as a background thread.
        """
        while True:
            time.sleep(self.cleanup_interval)
            self._perform_cleanup()

    def _perform_cleanup(self):
        """
        Remove keys that have expired based on their expiration times.
        """
        current_time = time.time()
        with self._lock:
            keys_to_delete = [key for key, exp in self._expirations.items() if current_time > exp]
            for key in keys_to_delete:
                del self._data[key]
                del self._expirations[key]

# Instantiate the InMemoryRedis client
redis_client = InMemoryRedis()
