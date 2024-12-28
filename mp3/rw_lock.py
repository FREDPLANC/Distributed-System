import threading
from contextlib import contextmanager

class ReadWriteLock:
    def __init__(self):
        self._read_lock = threading.Lock()  
        self._write_lock = threading.Lock()  
        self._readers = 0  

    @contextmanager
    def acquire_read(self):
        with self._read_lock:
            self._readers += 1
            if self._readers == 1:
                self._write_lock.acquire()
        try:
            yield  
        finally:
            with self._read_lock:
                self._readers -= 1
                if self._readers == 0:
                    self._write_lock.release()

    @contextmanager
    def acquire_write(self):
        self._write_lock.acquire()
        try:
            yield 
        finally:
            self._write_lock.release()
