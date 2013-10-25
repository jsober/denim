from unittest import TestCase
from diesel import quickstart, quickstop, Service


def upper(value):
    return value.upper()


class ClientTestCase(TestCase):
    def test_client(self):
        from denim.actors import Client, Manager
        from denim.protocol import Task

        result = {}

        def run_client():
            client = Client('localhost', 8888)
            with client:
                task = Task(upper, ['hello'])
                msgid = client.queue(task)
                result['reply'] = client.wait(msgid)
                quickstop()

        manager = Service(Manager(), 8888)
        quickstart(manager, run_client)

        self.assertIn('reply', result)
        self.assertRaises(Exception, result['reply'].get_result)
