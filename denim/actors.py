"""
TODO persistent storage of pending tasks?
"""
import logging
from collections import deque
from itertools import ifilter
from denim.protocol import Msg
from denim.util import Tracking


logging.basicConfig()
log = logging.getLogger()


class Dispatcher(object):
    def __init__(self, *args, **kwargs):
        self._dispatch = {}

    def can_respond_to(self, msg):
        return msg.cmd in self._dispatch

    def responds_to(self, cmd, cb):
        self._dispatch[cmd] = cb

    def dispatch(self, msg, *args, **kwargs):
        if not self.can_respond_to(msg):
            raise KeyError('Command not handled')
        self._dispatch[msg.cmd](msg, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        self.dispatch(*args, **kwargs)


class Actor(Dispatcher):
    def __init__(self, *args, **kwargs):
        super(Actor, self).__init__(*args, **kwargs)
        self.responds_to(Msg.QUEUE, self.handle_queue)
        self.responds_to(Msg.COLLECT, self.handle_collect)
        self.pending = {}
        self.complete = {}

    def handle_queue(self, msg, client, service):
        self.pending[msg.msgid] = msg
        service.reply(msg.reply(Msg.ACK))

    def handle_collect(self, msg, client, service):
        raise NotImplemented

    def set_complete(self, msg):
        del self.pending[msg.msgid]
        self.complete[msg.msgid] = msg


class Manager(Actor):
    def __init__(self, *args, **kwargs):
        super(Manager, self).__init__(*args, **kwargs)
        self.responds_to(Msg.REGISTER, self.handle_register)
        self.tracking = {}
        self.assigned = {}
        self.workers = {}

    def get_processing_time(self, worker):
        return self.tracking[worker.fd].processing_time

    def has_capacity(self, worker):
        return self.tracking[worker.fd].has_capacity

    def next_worker(self):
        filtered = ifilter(self.has_capacity, self.workers.values())
        workers = sorted(filtered, key=self.get_processing_time)
        if workers:
            return workers[0]

    def handle_register(self, msg, client, service):
        capacity = msg.payload['capacity']

        # Steal and configure pipe as worker
        service.steal_pipe(client)
        client.set_callbacks(self.worker_close, self.worker_msg)

        # Set up tracking and add to pool
        self.tracking[client.fd] = Tracking(capacity)
        self.workers[client.fd] = client

    def handle_queue(self, msg, client, service):
        worker = self.next_worker()
        if worker is not None:
            super(Manager, self).handle_queue(msg, client, service)
            worker.send(msg, self.worker_msg)
            self.tracking[worker.fd].start_tracking(msg.msgid)
            self.assigned[msg.msgid] = worker.fd
        else:
            service.reply(msg.reply(Msg.REJECTED))

    def worker_close(self, worker):
        raise NotImplemented
        for msgid in self.assigned.keys():
            if self.assigned[msgid] == worker.fd:
                reply = Msg(Msg.ERR, msgid, payload='Worker disconnected while processing message.')
                self.set_complete(reply)
                del self.assigned[msgid]

    def worker_msg(self, msg, worker):
        if msg.msgid in self.assigned:
            self.stop_tracking(msg.msgid)
            del self.assigned[msg.msgid]
            self.set_complete(msg)


class Worker(Actor):
    def __init__(self, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)
