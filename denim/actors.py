import diesel
from diesel.util.process import ProcessPool
from denim.protocol import Msg, Task, ProtocolError, protocol_error


class Client(diesel.Client):
    """
    This is the basic protocol layer used for all client connections.  It makes
    available all commands, regardless of the remote host's support for those
    commands.
    """
    _cmd_err = 'Invalid reply from server: (%d) %r'

    def __hash__(self):
        """
        This hash mechanism allows clusters to index workers by their
        connection information and prevents multiple active connections to the
        same worker.
        """
        return (self.addr, self.port)

    def _expect(self, msg, *args):
        """
        Helper function that raises a `ProtocolError` if `msg.cmd` is not
        present in `*args`. If `msg.cmd` is `Msg.ERR`, a protocol error is
        raised no matter what.
        """
        cmds = set(args)
        if msg.cmd == Msg.ERR:
            raise ProtocolError(msg.payload)
        elif msg.cmd in cmds:
            return True
        else:
            raise ProtocolError(self.cmd_err % (msg.cmd, msg.payload))

    def _next_msg(self):
        """
        Reads the next line off the wire and decodes it into a `Msg`.
        """
        return Msg.decode(diesel.until_eol())

    def _send(self, msg):
        """
        Encodes and sends a Msg.
        """
        diesel.send(msg.encode())

    def _cmd(self, cmd, expect_cmd, payload=None):
        """
        Sends a command to the remote host and returns either the reply with
        the expected command value or raises a `ProtocolError`.
        """
        msg = Msg(cmd, payload=payload)
        self._send(msg)
        reply = self._next_msg()
        self._expect(reply, expect_cmd)
        return reply

    @diesel.call
    def queue(self, task):
        """
        Sends a `Task` to the remote host. Returns the `msgid`.
        """
        reply = self._cmd(Msg.QUEUE, Msg.ACK, task)
        return reply.msgid

    @diesel.call
    def wait(self, msgid):
        """
        Cedes thread control until `msgid` is completed. Returns the result of
        the originally `Task` or raises an exception if the `Task` triggered
        one during processing.
        """
        reply = self._cmd(Msg.COLLECT, Msg.DONE, msgid)
        return reply.payload.get_result()

    @diesel.call
    def register(self, host, port):
        """
        Registers with a remote Manager. The remote host will connect back to
        the provided `host` and `port`.
        """
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
        """
        This is called by the diesel service before starting the loop. It lets
        the denim service learn the host and port on which the service is
        listening.
        """
        self.host = service.iface
        self.port = service.port

    def service_request(self, addr):
        """
        Reads a `Msg` in, attempts to dispatch it, and sends the result back.
        'Msg.cmd' values that are not registered via `responds_to` send a
        `Msg.ERR` message back.
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
        while True:
            self.service_request(addr)


class Worker(Service):
    def __init__(self, processes, manager=None):
        super(Worker, self).__init__()
        self.procs = processes
        self.is_managed = False

        if manager is not None:
            host, port = manager.split(':')
            self.mgr_host = host
            self.mgr_port = int(port)
            self.is_managed = True

        self.responds_to(Msg.QUEUE, self.handle_queue)

    def on_service_init(self, service):
        super(Worker, self).on_service_init(service)
        self.pool = ProcessPool(self.procs, self._worker)
        if self.is_managed:
            self.register()

    def register(self):
        if not self.is_managed:
            raise ValueError('No manager configured')

        client = Client(self.mgr_host, self.mgr_port)
        with client:
            client.register(self.host, self.port)

    def _worker(self, task):
        task.perform()
        return task

    def handle_queue(self, msg):
        if not isinstance(msg.payload, Task):
            return protocol_error('Invalid message payload')

        task = self.pool(msg.payload)
        return msg.reply(Msg.DONE, payload=task)


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