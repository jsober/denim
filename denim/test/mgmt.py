import socket
from functools import partial
from tornado.testing import AsyncTestCase
from tornado.iostream import IOStream


def connect(host, port, cb):
    stream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0))
    stream.connect((host, port), cb)
    return stream


class ServiceTestCase(AsyncTestCase):
    def test_service(self):
        from denim.mgmt import Service
        from denim.protocol import Msg, ProtocolError

        # 'msg' holds the msg to be sent to the service
        # 'reply' will hold the message returned by the service
        # 'c' will hold the client stream
        # 's' will hold the service instance
        state = {'msg': Msg(Msg.ACK), 'reply': None, 'c': None, 's': None}

        def on_connect(state):
            # Send msg to service
            state['c'].write(state['msg'].encode() + "\n")
            # Read reply from service
            state['c'].read_until("\n", partial(get_reply, state))

        def get_reply(state, line):
            # Decode reply from service
            state['reply'] = Msg.decode(line)
            self.stop()

        def client(state):
            # Connect to service
            state['c'] = connect('localhost', state['s'].port, partial(on_connect, state))

        def listen(state):
            # Start listening service
            state['s'] = Service()
            state['s'].start(1)

        # Init
        self.io_loop.add_callback(listen, state)
        self.io_loop.add_callback(client, state)
        self.wait()

        # Check results
        msg = state['msg']
        reply = state['reply']
        self.assertEqual(reply.msgid, msg.msgid)
        self.assertIsInstance(reply, Msg)
        self.assertEqual(reply.cmd, Msg.ERR)
        self.assertRaises(ProtocolError, reply.payload.get_result)
