import diesel
from denim.protocol import Msg, ProtocolError, protocol_error


class Client(diesel.Client):
    _cmd_err = 'Invalid reply from server: (%d) %r'

    def __hash__(self):
        return (self.addr, self.port)

    def _expect(self, msg, *args):
        cmds = set(args)
        if msg.cmd in cmds:
            return True
        elif msg.cmd == Msg.ERR:
            raise ProtocolError(msg.payload)
        else:
            raise ProtocolError(self.cmd_err % (msg.cmd, msg.payload))

    def _next_msg(self):
        return Msg.decode(diesel.until_eol())

    def _send(self, msg):
        diesel.send(msg.encode())

    def _cmd(self, cmd, expect_cmd, payload=None):
        msg = Msg(cmd, payload=payload)
        self._send(msg)
        reply = self._next_msg()
        self._expect(reply, expect_cmd)
        return reply

    @diesel.call
    def queue(self, task):
        reply = self._cmd(Msg.QUEUE, Msg.ACK, task)
        return reply.msgid

    @diesel.call
    def wait(self, msgid):
        reply = self._cmd(Msg.COLLECT, Msg.DONE, msgid)
        return reply.payload

    @diesel.call
    def register(self, host, port):
        self._cmd(Msg.REG, Msg.ACK, (host, port))
        return True


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


class Service(Dispatcher):
    """
    Provides a callable suitable to be passed to a diesel loop that implements
    the Dispatcher as a TCP/IP service.
    """
    def on_service_init(self, service):
        self.host = service.iface
        self.port = service.port

    def service_request(self, addr):
        """
        Reads a Msg in, attempts to dispatch it, and sends the result back.
        """
        line = diesel.until_eol()
        try:
            msg = Msg.decode(line)
        except ProtocolError, e:
            print e
            return

        try:
            reply = self.get_response(msg)
        except ProtocolError, e:
            reply = msg.reply(Msg.ERR, e)

        diesel.send(reply.encode())

    def __call__(self, addr):
        """
        Service loop.
        """
        while True:
            self.reader(addr)


class Worker(Service):
    def __init__(self, manager):
        super(Worker, self).__init__()
        host, port = manager.split(':')
        self.mgr_host = host
        self.mgr_port = int(port)

    def on_service_init(self, service):
        super(Worker, self).on_service_init(service)
        self.register()

    def register(self):
        client = Client(self.mgr_host, self.mgr_port)
        with client:
            client.register(self.host, self.port)


class Manager(Dispatcher):
    def __init__(self):
        super(Manager, self).__init__()
        self.workers = set()
        self.responds_to(Msg.REG, self.handle_reg)

    def handle_reg(self, msg):
        host, port = msg.payload
        client = Client(host, port)
        self.workers.add(client)
        return msg.reply(Msg.ACK)
