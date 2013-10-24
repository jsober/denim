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
    return sock, port


class Service(TCPServer):
    """
    Basic framework for a TCP/IP listening service using the denim protocol.
    Provides registration of callbacks based on the message command.
    """
    def __init__(self, port=None, *args, **kwargs):
        """
        Creates a service listening on port number `port`. If not specified,
        uses an OS-assigned port. All other arguments are transparently
        passed to the parent class' constructor.
        """
        super(Service, self).__init__(*args, **kwargs)
        self.dispatch = {}
        self.port = port

    def start(self, *args, **kwargs):
        """
        Starts the listening service. All arguments passed to parent.
        """
        if self.port:
            self.bind(self.port)
        else:
            sock, port = bind_unused_port()
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
            reply = self.get_response(Msg.decode(line))
        except Exception, e:
            reply = Msg(Msg.ERR, payload=protocol_error(e))

        stream.write(reply.encode() + "\n")
        self.reader(stream)


class Worker(Service):
    """
    Workers are leaf nodes in the denim network. They perform the tasks
    sent to them and return the results.
    """
    def __init__(self, processes=0, *args, **kwargs):
        """
        Creates a new worker service running `processes` number of forked
        processes. If not specified, forks 1 process for each cpu.
        """
        super(Node, self).__init__(*args, **kwargs)
        self.num_procs = processes
        self.responds_to(Msg.DO, self.do)

    def start(self, *args, **kwargs):
        """
        Starts the service. All arguments passed on to the parent.
        """
        super(Worker, self).start(self.num_procs, *args, **kwargs)

    def do(self, msg):
        """
        Handler for Msg.DO tasks.
        """
        if not msg.payload or not isinstance(msg.payload, Task):
            raise ProtocolError('Task not specified')

        msg.payload.perform()
        msg.cmd = Msg.DONE
        return msg
