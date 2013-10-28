from unittest import TestCase
import mock


def upper(value):
    """
    Simple test function for testing Tasks.
    """
    return value.upper()


class DispatcherTestCase(TestCase):
    def test_basic(self):
        from denim.actors import Dispatcher
        from denim.protocol import Msg

        results = {'test1': False, 'test2': True}

        def test1(msg):
            results['test1'] = msg
            return msg.reply(msg.ACK, payload=1)

        def test2(msg):
            results['test2'] = msg
            return msg.reply(msg.ACK, payload=2)

        d = Dispatcher()
        d.responds_to(Msg.ACK, test1)
        d.responds_to(Msg.REG, test2)

        self.assertEqual(d.get_response(Msg(Msg.ACK)).payload, 1)
        self.assertEqual(results['test1'].cmd, Msg.ACK)

        self.assertEqual(d.get_response(Msg(Msg.REG)).payload, 2)
        self.assertEqual(results['test2'].cmd, Msg.REG)


class ServiceTestCase(TestCase):
    port = 8888
    host = 'localhost'

    def test_on_service_init(self):
        from denim.actors import Service

        service = mock.Mock()
        service.iface = self.host
        service.port = self.port

        s = Service()
        s.on_service_init(service)
        self.assertEqual(s.port, self.port)
        self.assertEqual(s.host, self.host)

    @mock.patch('diesel.send')
    @mock.patch('diesel.until_eol')
    def test_service_request(self, until_eol, send):
        from denim.actors import Service
        from denim.protocol import Msg, ProtocolError

        result = {'reply': None}

        def side_effect(line):
            result['reply'] = Msg.decode(line)

        msg = Msg(Msg.QUEUE)
        send.side_effect = side_effect
        until_eol.return_value = msg.encode()

        s = Service()

        # Test error return
        s.service_request(None)
        self.assertIsInstance(result['reply'], Msg)
        self.assertEqual(result['reply'].cmd, Msg.ERR)
        self.assertIsInstance(result['reply'].payload, ProtocolError)

        # Reset result
        result['reply'] = None

        # Test correct dispatch
        fake_method = mock.Mock()
        fake_method.return_value = msg.reply(Msg.ACK)
        s.responds_to(Msg.QUEUE, fake_method)

        s.service_request(None)

        self.assertIsInstance(result['reply'], Msg)
        self.assertEqual(result['reply'].msgid, msg.msgid)


class WorkerTestCase(TestCase):
    mgr_addr = 'localhost:8888'
    mgr_port = 8888
    mgr_host = 'localhost'

    wkr_port = 8889
    wkr_host = 'localhost'

    def test_worker_init(self):
        from denim.actors import Worker

        worker = Worker(None, self.mgr_addr)
        self.assertEqual(worker.mgr_host, self.mgr_host)
        self.assertEqual(worker.mgr_port, self.mgr_port)

    @mock.patch('denim.actors.ProcessPool')
    @mock.patch('denim.actors.Worker.register')
    def test_managed_worker_on_service_init(self, register, pool):
        from denim.actors import Worker

        worker = Worker(4, self.mgr_addr)
        worker.on_service_init(mock.Mock())
        pool.assert_called_once_with(4, worker._worker)
        register.assert_called_once_with()

    @mock.patch('denim.actors.ProcessPool')
    @mock.patch('denim.actors.Worker.register')
    def test_standalone_worker_on_service_init(self, register, pool):
        from denim.actors import Worker

        worker = Worker(4)
        worker.on_service_init(mock.Mock())
        pool.assert_called_once_with(4, worker._worker)
        self.assertEqual(len(register.mock_calls), 0)

    @mock.patch('denim.actors.Client')
    def test_worker_register(self, client):
        client.register = mock.Mock()

    @mock.patch('denim.actors.ProcessPool')
    @mock.patch('denim.actors.Worker.register')
    def test_handle_queue(self, register, pool):
        pass
