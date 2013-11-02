from tornado.testing import AsyncTestCase


class NetTestCase(AsyncTestCase):
    def test(self):
        from denim.net import Service, Client
        from denim.protocol import Msg

        results = {
            'c_conn': None,
            'c_msg_sent': None,
            'c_msg_pend': None,
            'c_reply': None,
            'c_close': None,

            's_msg_pend': None,
            's_reply': None,
        }

        def s_on_msg(msg, service):
            results['s_msg_pend'] = s.is_pending(msg.msgid)
            reply = msg.reply(Msg.ACK)
            s.reply(reply)
            results['s_reply'] = reply

        def c_on_conn():
            # This avoids OS-level caching to optimize bandwidth, but helps to
            # ensure faster tests.
            c.pipe.set_nodelay(1)

            results['c_conn'] = c.is_connected
            msg = Msg(Msg.QUEUE)
            c.send(msg, c_on_reply)
            results['c_msg_pend'] = c.is_pending(msg.msgid)
            results['c_msg_sent'] = msg

        def c_on_reply(reply):
            results['c_reply'] = reply
            s.stop()
            c.close()
            self.stop()

        def c_on_close():
            results['c_close'] = True

        def c_on_msg():
            pass

        s = Service(s_on_msg)
        self.assertTrue(not s.is_running)

        s.start()
        self.assertTrue(s.port)
        self.assertTrue(s.host)
        self.assertTrue(s.is_running)

        c = Client(s.host, s.port, c_on_conn, c_on_close, c_on_msg)
        self.assertTrue(not c.is_connected)
        c.connect()

        self.wait()
        self.assertTrue(results['c_conn'])
        self.assertTrue(results['c_msg_sent'].cmd == Msg.QUEUE)
        self.assertTrue(results['c_msg_pend'])
        self.assertTrue(results['c_reply'])
        self.assertTrue(results['c_close'])

        self.assertTrue(results['s_reply'].cmd == Msg.ACK)
        self.assertTrue(results['s_msg_pend'])

        self.assertTrue(results['s_reply'].msgid == results['c_msg_sent'].msgid)
