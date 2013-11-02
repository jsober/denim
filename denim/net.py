import socket
from tornado.ioloop import PeriodicCallback
from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer
from tornado.netutil import bind_sockets
from denim.protocol import Msg


class NotConnectedError(Exception):
    """Raised when an operation is attempted on a disconnected channel.
    """
    pass


class Pipe(object):
    """Wraps a `tornado.iostream.IOStream` object. A Pipe monitors a stream for
    incoming messages and triggers a callback whenever one is received.
    Messages sent through a Pipe that provide a callback are tracked and bypass
    the general message callback (e.g. sending a request and providing a
    callback expecting the reply).
    """
    def __init__(self, stream, close_cb, msg_cb):
        """Creates a new `Pipe` wrapping a connected `IOStream`. The `close_cb`
        callback will be triggered when a disconnection is detected. The
        `mes_cb` callback is triggered whenever a new, untracked message is
        received (where untracked means that no prior message had been sent
        using `send` with the same message id.
        """
        self.stream = stream
        self.close_cb = close_cb
        self.msg_cb = msg_cb
        self.stream.set_close_callback(self.close)
        self.pending = {}
        self.schedule_read()
        self.ping = PeriodicCallback(self.send_ping, 5 * 1000)

    def __hash__(self):
        return self.fd

    @property
    def fd(self):
        return self.stream.fileno()

    def send_ping(self):
        self.send(Msg(Msg.PING), self.recv_ping)

    def recv_ping(self):
        pass

    def set_callbacks(self, close_cb, msg_cb):
        self.close_cb = close_cb
        self.msg_cb = msg_cb

    @property
    def is_connected(self):
        """Returns True if the client stream is defined.
        """
        return self.stream is not None

    def is_pending(self, msgid):
        """Returns True if the msgid refers to a Msg which has been sent by
        this client.
        """
        return msgid in self.pending

    def set_nodelay(self, flag):
        """Sets the nodelay option on the underlying socket. This prevents the
        OS from caching socket traffic in to make more efficient use of
        bandwidth, but speeds up individual transmissions. Generally a good
        idea to leave it alone, but it is useful for applications that require
        realtime monitoring of transmissions, such as unit tests.
        """
        if self.is_connected:
            self.stream.set_nodelay(flag)

    def close(self):
        """Closes the stream and triggers and triggers the `close_cb` callback.
        Although this method is also triggered by the stream's close callback,
        it will not execute twice because the first execution will set the
        stream attribute to None, preventing the method from executing again.
        """
        if self.is_connected:
            self.close_cb(self)
            self.stream.close()
            self.stream = None

    def send(self, msg, cb=None):
        """Sends a message to the remote host, and triggers `cb` with the reply
        once the server responds.
        """
        if not self.is_connected:
            raise NotConnectedError

        data = msg.encode() + "\n"
        self.stream.write(data)
        if cb is not None:
            self.pending[msg.msgid] = cb

    def on_message(self, line):
        """Triggered by the stream (via `schedule_read`) when a line of data
        has been buffered and is ready. Decodes the line into a Msg object and
        then routes it based on whether or not the msgid is known.

        If known, it triggers the callback originally passed to `send` for this
        msgid. Otherwise, it triggers the generic `msg_cb` callback property.

        The one exception is a PING msg. These messages are immediately handled
        and an ACK reply is sent.
        """
        msg = Msg.decode(line)

        if msg.cmd == Msg.PING:
            self.send(msg.reply(Msg.ACK))
        elif self.is_pending(msg.msgid):
            cb = self.pending[msg.msgid]
            del self.pending[msg.msgid]
            cb(msg, self)
        else:
            self.msg_cb(msg, self)

        self.schedule_read()

    def schedule_read(self):
        """Registers a read event watcher in the stream. The stream will trigger
        the `on_message` callback once a complete line of data has been buffered.
        """
        if self.is_connected:
            self.stream.read_until("\n", self.on_message)


class Service(object):
    """A TCP/IP listening service that responds to `denim.protocol.Msg`s.
    """
    def __init__(self, msg_cb, port=None, host='localhost', procs=1):
        """Creates a new service that calls `msg_cb` with each new request
        received. If provided, the service will listen on `host`:`port`.
        Otherwise, an OS-provided port will be used. Optional parameter
        `procs` starts multiple processes.
        """
        self.msg_cb = msg_cb
        self.port = port
        self.host = host
        self.procs = procs
        self.clients = {}
        self.pending = {}
        self.server = None

    @property
    def is_running(self):
        """Returns True if the server has been started.
        """
        return self.server is not None

    def is_pending(self, msgid):
        """Returns True if `msgid` is awaiting a response.
        """
        return msgid in self.pending

    def build_socket(self):
        """Builds the listener socket. If `self.port` is not set, uses an
        OS-assigned port. Note that the `host` and `port` values are updated
        by the `start` method.
        """
        [sock] = bind_sockets(self.port, self.host, family=socket.AF_INET)
        host, port = sock.getsockname()
        return sock, port, host

    def start(self):
        """Starts the service on the configured port and host, or uses an
        OS-assigned port if not provided.
        """
        sock, port, host = self.build_socket()
        self.port = port
        self.host = host

        self.server = TCPServer()
        self.server.handle_stream = self.handle_stream
        self.server.add_socket(sock)
        self.server.start(self.procs)

    def stop(self):
        """Stops the TCP/IP listening service.
        """
        self.server.stop()
        self.server = None

    def handle_stream(self, stream, addr):
        """Triggered by the listener when a new client connects. This method
        starts the read loop.
        """
        pipe = Pipe(stream, self.cleanup, self.on_receive)
        self.clients[pipe.fd] = pipe

    def cleanup(self, client):
        """Cleans up after a client connection.
        """
        del self.clients[client.fd]
        for msgid in self.pending.keys():
            if self.pending[msgid] == client.fd:
                # TODO trigger some sort of cancel callback so the logic
                # managing the Service can deal with a cancelled msg.
                del self.pending[msgid]

    def on_receive(self, msg, client):
        """Receives a line of data from the stream referred to by `addr`. The
        `msg_cb` callback is then called with the message object, the pipe from
        which the message was received, and a reference to the service itself.
        """
        self.pending[msg.msgid] = client.fd
        self.msg_cb(msg, client, self)

    def reply(self, msg):
        """This method is used by the caller to respond to a `Msg` that has
        been received. A `KeyError` is raised if the message id is not
        recognized.
        """
        if not self.is_pending(msg.msgid):
            raise KeyError('msgid not recognized')

        fd = self.pending[msg.msgid]
        pipe = self.clients[fd]
        pipe.send(msg)
        del self.pending[msg.msgid]

    def steal_pipe(self, pipe):
        """Allows the caller to "steal" a `Pipe` that is currently being
        managed as a client. This is particularly useful if a service wishes to
        handle certain clients differently (such as a `Manager` does when a
        `Worker` registers.)
        """
        self.cleanup(pipe)


class Client(object):
    """A client connection to a denim service.
    """
    def __init__(self, host, port, connect_cb, close_cb, msg_cb):
        """Creates a client to a remote service at `host`:`port`. Three
        callbacks are required. `connect_cb` is triggered once when the client
        has connected. `close_cb` is triggered once when the client connection
        is closed. `msg_cb` is triggered when a message is received for which
        no earlier message was sent (see `send`).
        """
        self.port = port
        self.host = host
        self.connect_cb = connect_cb
        self.pipe_args = [close_cb, msg_cb]
        self.pipe = None

    @property
    def is_connected(self):
        """Returns True if the client stream is defined.
        """
        return self.pipe is not None and self.pipe.is_connected

    def is_pending(self, msgid):
        """Returns True if the msgid refers to a Msg which has been sent by
        this client.
        """
        return self.pipe is not None and self.pipe.is_pending(msgid)

    def connect(self):
        """Connects to the remote host and triggers the `connect_cb` callback
        that was provided to the constructor.
        """
        if self.is_connected:
            self.close()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        stream = IOStream(sock)
        stream.connect((self.host, self.port), self.on_connect)
        self.pipe = Pipe(stream, *self.pipe_args)

    def close(self):
        """Closes the underlying connection.
        """
        self.pipe.close()

    def send(self, msg, cb):
        """Sends `msg` to the remote and triggers `cb` with the reply once it
        has been received.
        """
        self.pipe.send(msg, cb)

    def on_connect(self):
        """Triggered when the connection is established.
        """
        self.connect_cb()
