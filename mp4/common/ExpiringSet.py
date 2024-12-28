import heapq
import time

class ExpiringSet:
    def __init__(self, expired_time = 2):
        self.active_set = set()        
        self.expiry_queue = []           
        self.expired_time = expired_time
    def add(self, item, time_stamp):
        expire_time = time_stamp + self.expired_time
        self.active_set.add(item)
        heapq.heappush(self.expiry_queue, (expire_time, item))

    def remove_expired(self):
        current_time = time.time()
        while self.expiry_queue and self.expiry_queue[0][0] <= current_time:
            expire_time, item = heapq.heappop(self.expiry_queue)
            if item in self.active_set: 
                self.active_set.remove(item)

    def __contains__(self, item):
        return item in self.active_set

    def __repr__(self):
        self.remove_expired()
        return repr(self.active_set)
