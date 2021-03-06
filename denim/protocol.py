try:
    import cPickle as pickle
except:
    import pickle

import base64
import uuid


class ProtocolError(Exception):
    def __init__(self, msg, cmd=None):
        super(ProtocolError, self).__init__(msg)
        self.cmd = cmd

    def __str__(self):
        msg = super(ProtocolError, self).__str__()
        return '(%d) %s' % (self.cmd, msg)

    def __unicode__(self):
        msg = super(ProtocolError, self).__unicode__()
        return '(%d) %s' % (self.cmd, msg)


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
        return self

    def get_result(self):
        if self.is_error:
            raise self.result
        else:
            return self.result


class Msg(object):
    # Response commands
    ERROR = 0
    ACK = 1
    DONE = 2
    NOT_DONE = 3

    # Request commands
    REGISTER = 4
    QUEUE = 5
    COLLECT = 6
    PING = 7
    REJECTED = 8

    def __init__(self, cmd, msgid=None, payload=None):
        if msgid is None:
            msgid = str(uuid.uuid4())

        self.cmd = cmd
        self.msgid = msgid
        self.payload = payload

    def __str__(self):
        return '<Msg (%d) %s>' % (self.cmd, self.msgid)

    def __unicode__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.msgid == other.msgid

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

