from collections import deque
import time


class Tracker(object):
    def __init__(self, capacity, count=10):
        self.max_count = count
        self.count = 0
        self.data = deque()
        self.tracking = {}
        self._capacity = capacity
        self._avg = 0

    def start_tracking(self, msgid):
        self.tracking[msgid] = time.now()

    def stop_tracking(self, msgid):
        now = time.now()
        then = self.tracking[msgid]

        while self.count >= self.max_count:
            self.data.popLeft()
            self.count -= 1

        self.data.append(now - then)
        self.count += 1
        self.recalculate_avg()

        del self.tracking[msgid]

    def recalculate_avg(self):
        if self.count == 0:
            self._avg = 0
        self._avg = sum(self.data) / self.count

    @property
    def num_pending(self):
        return len(self.tracking.keys())

    @property
    def avg(self):
        return self._avg

    @property
    def processing_time(self):
        return self.avg * (self.num_pending + 1)

    @property
    def capacity(self):
        return self._capacity - self.num_pending

    @property
    def has_capacity(self):
        return self._capacity < self.num_pending
