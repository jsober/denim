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
    def test_service_request_ping(self, until_eol, send):
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
        msg = Msg(Msg.PING)
        until_eol.return_value = msg.encode()
        s.service_request(None)

        self.assertIsInstance(result['reply'], Msg)
        self.assertEqual(result['reply'].cmd, Msg.ACK)
        self.assertEqual(result['reply'].msgid, msg.msgid)


class WorkerTestCase(TestCase):
    procs = 4
    mgr_addr = 'localhost:8888'
    mgr_port = 8888
    mgr_host = 'localhost'

    def _mock_client_instance(self, client):
        inst = mock.Mock()
        client.return_value = inst
        inst.__enter__ = mock.Mock()
        inst.__exit__ = mock.Mock()
        inst.__enter__.return_value = inst
        return inst

    def test_worker_init(self):
        from denim.actors import Worker
        worker = Worker(self.procs, self.mgr_addr)
        host, port = worker.manager_addr()
        self.assertEqual(host, self.mgr_host)
        self.assertEqual(port, self.mgr_port)

    @mock.patch('denim.actors.ProcessPool')
    @mock.patch('denim.actors.Worker.register')
    def test_worker_on_service_init(self, register, pool):
        from denim.actors import Worker
        worker = Worker(self.procs, self.mgr_addr)
        worker.on_service_init(mock.Mock())
        pool.assert_called_once_with(self.procs, worker._worker)
        register.assert_called_once_with()

    @mock.patch('denim.actors.Client')
    def test_worker_register(self, client):
        from denim.actors import Worker

        inst = self._mock_client_instance(client)
        inst.register = mock.Mock()

        worker = Worker(self.procs, self.mgr_addr)
        worker.host = 'localhost'
        worker.port = 8001
        worker.register()

        client.assert_called_once_with(self.mgr_host, self.mgr_port, timeout=worker.timeout)
        inst.register.assert_called_once_with('localhost', 8001)

    @mock.patch('diesel.sleep')
    @mock.patch('denim.actors.Client')
    def test_worker_register_retry(self, client, sleep):
        from diesel import ClientConnectionError
        from denim.actors import Worker

        inst = self._mock_client_instance(client)
        inst.register = mock.Mock()

        def connect_fail(*args, **kwargs):
            def connect_ok(*args, **kwargs):
                return inst
            client.side_effect = connect_ok
            raise ClientConnectionError('mock connection error')

        client.side_effect = connect_fail

        worker = Worker(self.procs, self.mgr_addr)
        worker.host = 'localhost'
        worker.port = 8001
        worker.register()

        sleep.assert_called_once_with(worker.reconnect_retry_time)

        self.assertEqual(client.call_args_list, [
            mock.call(self.mgr_host, self.mgr_port, timeout=worker.timeout),
            mock.call(self.mgr_host, self.mgr_port, timeout=worker.timeout),
        ])

        inst.register.called_once_with('localhost', 8001)

    @mock.patch('denim.actors.Service.get_response')
    def test_get_response_from_mgr(self, get_response):
        from denim.actors import Worker

        worker = Worker(self.procs, self.mgr_addr)
        worker.get_response(None, self.mgr_addr)
        get_response.assert_called_once_with(None, self.mgr_addr)

    def test_get_response_from_other(self):
        from denim.actors import Worker
        from denim.protocol import Msg, Task, ProtocolError

        msg = Msg(Msg.PING)
        worker = Worker(self.procs, self.mgr_addr)
        reply = worker.get_response(msg, 'fail')

        self.assertEqual(reply.cmd, Msg.ERR)
        self.assertIsInstance(reply.payload, Task)
        self.assertRaises(ProtocolError, reply.payload.get_result)

    @mock.patch('denim.actors.ProcessPool')
    def test_handle_queue_task(self, pool):
        from denim.actors import Worker
        from denim.protocol import Msg, Task

        task = Task(lambda: 42)
        msg = Msg(Msg.QUEUE, payload=task)
        worker = Worker(self.procs, self.mgr_addr)
        worker.pool = pool()

        reply = worker.handle_queue(msg)
        self.assertIsInstance(reply, Msg)
        self.assertEqual(reply.cmd, Msg.DONE)

    def test_handle_queue_non_task(self):
        pass
