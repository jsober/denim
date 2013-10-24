from functools import partial
from tornado.tcpserver import TCPServer
from tornado import netutil
import socket

from denim.protocol import Msg, protocol_error


def bind_unused_port():
    """
    Creates a listening socket with an OS-assigned port. Returns tuple of
    (socket, port_number).
    """
    [sock] = netutil.bind_sockets(None, 'localhost', family=socket.AF_INET)
    port = sock.getsockname()[1]
    host = sock.getsockname()[0]
    return sock, host, port


class Service(TCPServer):
    """
    Basic framework for a TCP/IP listening service using the denim protocol.
    Provides registration of callbacks based on the message command.
    """
    def __init__(self, port=None, host=None,*args, **kwargs):
        """
        Creates a service listening on port number `port`. If not specified,
        uses an OS-assigned port. All other arguments are transparently
        passed to the parent class' constructor.
        """
        super(Service, self).__init__(*args, **kwargs)
        self.dispatch = {}
        self.port = port
        self.host = host

    def start(self, *args, **kwargs):
        """
        Starts the listening service. All arguments passed to parent.
        """
        if self.port:
            self.bind(self.port, self.host)
        else:
            sock, host, port = bind_unused_port()
            self.host = host
            self.port = port
            self.add_sockets([sock])

        super(Service, self).start(*args, **kwargs)

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

    def handle_stream(self, stream, address):
        """
        Called by parent class when a new client connection is accepted.
        """
        self.reader(stream)

    def reader(self, stream):
        """
        Begins request/response cycle.
        """
        stream.read_until("\n", partial(self.dispatcher, stream))

    def dispatcher(self, stream, line):
        """
        Dispatches requests and writes the reply generated back to the client
        stream, then starts the cycle over again.
        """
        try:
            msg = Msg.decode(line)
        except Exception, e:
            reply = Msg(Msg.ERR, payload=protocol_error(e))

        try:
            reply = self.get_response(msg)
        except Exception, e:
            reply = msg.reply(Msg.ERR, payload=protocol_error(e))

        stream.write(reply.encode() + "\n")
        self.reader(stream)


class Worker(Service):
    """
    Workers are leaf nodes in the denim network. They perform the tasks
    sent to them and return the results. Workers can run either standalone
    or slaved to a Manager.
    """
    def __init__(self, processes=0, manager=None, *args, **kwargs):
        """
        Creates a new worker service running `processes` number of forked
        processes. If not specified, forks 1 process for each cpu. The
        Worker's manager is specified as host:port. If not specified, the
        Worker is run in standalone mode.
        """
        super(Worker, self).__init__(*args, **kwargs)
        self.num_procs = processes
        self.manager_addr = None
        self.manager = None

        self.responds_to(Msg.DO, self.respond_do)

    def start(self, *args, **kwargs):
        """
        Starts the service. All arguments passed on to the parent.
        """
        super(Worker, self).start(self.num_procs, *args, **kwargs)

    def register(self):
        host, port = self.manager_addr.split(':')
        reg_msg = Msg(Msg.REG, payload=(host, port))
        self.manager = Client(host, port, self.on_mgr_msg)
        self.manager.send(reg_msg, start_ping)

    def start_ping(self, msg):
        pass

    def on_mgr_msg(self, msg):
        pass

    def respond_do(self, msg):
        """
        Handler for Msg.DO tasks.
        """
        if not msg.payload or not isinstance(msg.payload, Task):
            raise ProtocolError('Task not specified')

        msg.payload.perform()
        msg.cmd = Msg.DONE
        return msg


class Manager(Service):
    def __init__(self, *args, **kwargs):
        super(Manager, self).__init__(*args, **kwargs)
        self.responds_to(Msg.REG, self.respond_reg)
        self.responds_to(Msg.QUEUE, self.respond_queue)

    def start(self, *args, **kwargs):
        super(Manager, self).start(1, *args, **kwargs)

    def respond_reg(self, msg):
        raise ProtocolError('Not implemented')

    def respond_queue(self, msg):
        raise ProtocolError('Not implemented')


class Client(Service):
    def __init__(self, host, port, on_msg):
        self.host = host
        self.port = port
        self.pending = dict()
        self.msg_cb = on_msg

    def connect(self):
        self.stream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0))
        self.stream.connect((self.host, self.port), self.on_connect)

    def on_connect(self):
        pass

    def reader(self, stream):
        self.stream.read_until("\n", self.on_message)

    def on_message(self, line):
        msg = Msg.decode(line)
        if msg.msgid in self.pending:
            cb = self.pending[msg.msgid]
            del self.pending[msg.msgid]
            if cb is not None:
                cb(msg)
        else:
            self.msg_cb(msg)

        self.reader()

    def send(self, msg, cb=None):
        self.stream.write(msg.encode() + "\n")
        self.pending[msg.msgid] = cb
