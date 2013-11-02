import heapq
from denim.actors import Msg


class MsgQueue(object):
    class Empty(Exception):
        pass

    class Full(Exception):
        pass

    def __init__(self, size):
        self.size = size
        self.count = 0
        self.msgs = []

    @property
    def is_full(self):
        return self.count >= self.items

    @property
    def is_empty(self):
        return self.count == 0

    def peek(self):
        if self.is_empty:
            raise MsgQueue.QueueEmpty('queue empty')

        (pri, msg) = self.msgs[0]
        return msg

    def put(self, msg):
        if not isinstance(msg, Msg):
            raise ValueError('not a Msg')

        if self.is_full:
            raise MsgQueue.QueueFull('queue full')

        heapq.heappush(self.msgs, (msg.priority, msg))
        self.count += 1

    def get(self):
        if self.is_empty:
            raise MsgQueue.QueueEmpty('queue empty')

        (pri, msg) = heapq.heappop(self.msgs)
        self.count -= 1

        return msg
