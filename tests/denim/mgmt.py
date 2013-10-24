import socket
import unittest
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream


class ServiceTestCase(unittest.TestCase):
    port = 9999
    loop = IOLoop.instance()

    def test_service(self):
        from denim.mgmt import Service
        from denim.protocol import Msg, ProtocolError

        reply = None

        def send_msg():
            msg = Msg(Msg.ACK)
            c.write(msg.encode + "\n")
            c.read_until("\n", get_reply)

        def get_reply(line):
            reply = Msg.decode(line)
            self.loop.stop()

        # Start server
        s = Service(port=self.port)
        s.start()

        # Start client
        c = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0))
        c.connect(("localhost", self.port), send_msg)

        self.loop.start()

        self.assertInstance(reply, Msg)
        self.assertEqual(reply.cmd, Msg.ERR)
        self.assertRaises(ProtocolError, reply.payload.get_result)
