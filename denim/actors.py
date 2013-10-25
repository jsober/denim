import diesel
from denim.protocol import Msg, ProtocolError, protocol_error


class Client(diesel.Client):
    def __init__(self, *args, **kwargs):
        super(Client, self).__init__(*args, **kwargs)
        self.complete = {}

    def receive(self):
        line = diesel.until_eol()
        msg = Msg.decode(line)
        self.complete[msg.msgid] = msg

    @diesel.call
    def queue(self, task):
        msg = Msg(Msg.QUEUE, payload=task)
        diesel.send(msg.encode())
        return msg.msgid

    @diesel.call
    def wait(self, msgid):
        while msgid not in self.complete:
            self.receive()

        result = self.complete[msgid]
        del self.complete[msgid]

        return result.payload


class Dispatcher(object):
    """
    Dispatches messages based on the message cmd.
    """
    def __init__(self):
        self.dispatch = {}

    def responds_to(self, cmd, cb):
        """
        Registers a callback for messages based on the message command.
        The callback must return a Message object.
        """
        self.dispatch[cmd] = cb

    def get_response(self, msg):
        """
        Attempts to call the handler callback for a given message based
        on the message command.
        """
        if msg.cmd in self.dispatch:
            reply = self.dispatch[msg.cmd](msg)
            if not isinstance(reply, Msg):
                raise ProtocolError('The server generated an invalid response')
        else:
            raise ProtocolError('Command not handled')

        return reply


class Manager(Dispatcher):
    def __init__(self):
        Dispatcher.__init__(self)

    def __call__(self, addr):
        while True:
            line = diesel.until_eol()

            try:
                msg = Msg.decode(line)
            except ProtocolError, e:
                print e
                continue

            try:
                reply = self.get_response(msg)
            except ProtocolError, e:
                reply = msg.reply(Msg.ERR, protocol_error(e))

            diesel.send(reply.encode())
