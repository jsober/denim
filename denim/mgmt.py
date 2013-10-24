from functools import partial
from tornado.tcpserver import TCPServer
from denim.protocol import Msg, protocol_error


class Service(TCPServer):
    def __init__(self, port, *args, **kwargs):
        super(Service, self).__init__(*args, **kwargs)
        self.dispatch = {}
        self.port = port

    def start(self, processes=0):
        self.bind(self.port)
        self.start(processes)

    def responds_to(self, cmd, cb):
        self.dispatch[cmd] = cb

    def get_response(self, msg):
        if msg.cmd in self.dispatch:
            return self.dispatch[msg.cmd](msg)
        else:
            return protocol_error('Command not handled')

    def handle_stream(self, stream, address):
        self._read_request(stream)

    def _read_request(self, stream):
        stream.read_until("\n", partial(self._dispatch, stream))

    # TODO trap decoding errors and log them
    def _dispatch(self, stream, line):
        msg = Msg.decode(line)
        reply = self.get_response(msg)
        self.write(reply.encode + "\n")
        self._read_request(stream)


class Worker(Service):
    def __init__(self, processes=0, *args, **kwargs):
        super(Node, self).__init__(*args, **kwargs)
        self.num_procs = processes
        self.responds_to(Msg.DO, self.do)

    def start(self):
        super(Worker, self).start(self.num_procs)

    def do(self, msg):
        msg.payload.perform()
        msg.cmd = Msg.DONE
        return msg
