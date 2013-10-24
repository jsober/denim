try:
    import cPickle as pickle
except:
    import pickle

import base64
import uuid


def protocol_error(msg, exc=None):
    if exc is None:
        exc = ProtocolError(msg)

    task = Task(0)
    task.is_error = True
    task.result = exc

    return task


class ProtocolError(Exception):
    pass


class Task(object):
    def __init__(self, f, args=None, kwargs=None):
        if args is None:
            args = []

        if kwargs is None:
            kwargs = {}

        self.f = f
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.is_error = False

    def perform(self):
        try:
            self.result = self.f(*self.args, **self.kwargs)
        except Exception, e:
            self.result = e
            self.is_error = True

    def get_result(self):
        if self.is_error:
            raise self.result
        else:
            return self.result


class Msg(object):
    ERR = 0
    ACK = 1
    DO = 2
    DONE = 3

    def __init__(self, cmd, msgid=None, payload=None):
        if msgid is None:
            msgid = str(uuid.uuid4())

        self.cmd = cmd
        self.msgid = msgid
        self.payload = payload

    def encode(self):
        return "%d|%s|%s" % (
            self.cmd,
            self.msgid,
            base64.b64encode(pickle.dumps(self.payload)),
        )

    @staticmethod
    def decode(line):
        try:
            cmd, msgid, data = line.split('|')
            payload = pickle.loads(base64.b64decode(data))
            return Msg(int(cmd), msgid, payload)
        except:
            raise ProtocolError('Invalid message format')

    def reply(self, cmd, payload=None):
        return Msg(cmd, self.msgid, payload)

