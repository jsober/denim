import unittest

def _do_work(msg):
    return msg.upper()

def _do_fail():
    raise ValueError('test error')


class TaskTestCase(unittest.TestCase):
    def test_task(self):
        from denim.protocol import Task

        task = Task(_do_work, ['hello'])
        task.perform()

        self.assertFalse(task.is_error)

        result = task.get_result()
        self.assertEqual(result, 'HELLO')

    def test_task_fail(self):
        from denim.protocol import Task

        task = Task(_do_fail)
        task.perform()

        self.assertRaises(ValueError, task.get_result)


class MsgTestCase(unittest.TestCase):
    def test_encode_decode(self):
        from denim.protocol import Msg

        msg = Msg(Msg.ACK, payload='How now brown bureaucrat')
        self.assertTrue(msg.msgid)

        decoded = Msg.decode(msg.encode())

        self.assertIsInstance(decoded, Msg)
        self.assertEqual(decoded.payload, msg.payload)
        self.assertEqual(decoded.msgid, msg.msgid)
        self.assertEqual(decoded.cmd, msg.cmd)

    def test_reply(self):
        from denim.protocol import Msg

        msg = Msg(Msg.ACK)
        reply = msg.reply(Msg.ERR, payload='foo')

        self.assertEqual(msg.msgid, reply.msgid)
        self.assertEqual(reply.payload, 'foo')
        self.assertEqual(reply.cmd, Msg.ERR)

    def test_task_workflow(self):
        from denim.protocol import Msg, Task

        task = Task(_do_work, ['hello'])
        msg = Msg(Msg.ACK, payload=task)
        send_line = msg.encode()

        recv = Msg.decode(send_line)
        recv.payload.perform()
        recv_line = recv.encode()

        reply = Msg.decode(recv_line)
        result = reply.payload.get_result()

        self.assertEqual(result, 'HELLO')
