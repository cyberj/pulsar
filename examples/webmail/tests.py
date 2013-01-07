from pulsar import send, get_application, is_async
from pulsar.apps.test import unittest, run_on_arbiter

try:
    from .manage import server, email_client
    has_twisted = True
except ImportError:
    has_twisted = False

@unittest.skipUnless(has_twisted, 'Requires twisted')
class TestWebMailThread(unittest.TestCase):
    app = None
    concurrency = 'thread'
    
    @classmethod
    def name(cls):
        return 'webmail_' + cls.concurrency
    
    @classmethod
    def setUpClass(cls):
        name = cls.name()
        kwargs = {'%s__bind' % name: '127.0.0.1:0'}
        s = server(name=cls.name(), concurrency=cls.concurrency, **kwargs)
        outcome = send('arbiter', 'run', s)
        yield outcome
        cls.app = outcome.result
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            outcome = send('arbiter', 'kill_actor', cls.app.name)
            yield outcome
    
    @run_on_arbiter
    def testEmailClient(self):
        app = get_application(self.name())
        ec = email_client(app.cfg.imap)
        self.assertTrue(is_async(ec))
        yield ec
        client = ec.result
        self.assertTrue(client)