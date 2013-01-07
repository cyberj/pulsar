'''Test twisted integration'''
import socket

import pulsar
from pulsar import is_failure, is_async
from pulsar.utils.httpurl import to_bytes, to_string
from pulsar.apps.socket import SocketServer
from pulsar.apps.test import unittest, run_on_arbiter, dont_run_with_thread

try:
    import twisted
    from twisted.internet.protocol import Factory, Protocol
    from twisted.internet.defer import Deferred
    
    class EchoClient(Protocol):
        result = None
        def sendMessage(self, msg):
            r = self.result
            if r is not None:
                return r.addCallback(lambda res: self.sendMessage(msg))
            else:
                self.result = r = Deferred()
                t = self.transport
                t.write(msg)
                return r
            
        def dataReceived(self, data):
            r = self.result
            self.result = None
            if r:
                r.callback(data)
            
        
    class EchoClientFactory(Factory):
        protocol = EchoClient
except:
    twisted = None
        
        
class EchoResponse(pulsar.AsyncResponse):
    
    def __iter__(self):
        if self.parsed_data == b'quit':
            yield b'bye'
            self.connection.close()
        else:
            yield self.parsed_data
            
            
class TestServerSocketServer(pulsar.AsyncSocketServer):
    response_class = EchoResponse
    
    
@unittest.skipUnless(twisted, 'Requires twisted')
class TestTwistedIntegration(unittest.TestCase):
    concurrency = 'thread'
    server = None
    @classmethod
    def setUpClass(cls):
        s = SocketServer(socket_server_factory=TestServerSocketServer,
                         name=cls.__name__.lower(), bind='127.0.0.1:0',
                         concurrency=cls.concurrency)
        outcome = pulsar.send('arbiter', 'run', s)
        yield outcome
        cls.server = outcome.result
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
        
    def client(self, **kwargs):
        from twisted.internet.endpoints import TCP4ClientEndpoint
        from pulsar.async.tx import pulsar_reactor
        pulsar_reactor.run()
        point = TCP4ClientEndpoint(pulsar_reactor, "127.0.0.1",
                                   self.server.address[-1])
        return point.connect(EchoClientFactory())
        
    def testEchoClient(self):
        client = self.client()
        self.assertTrue(is_async(client))
        yield client
        client = client.result
        future = client.sendMessage("Hello")
        yield future
        self.assertEqual(future.result, 'Hello')
        future = client.sendMessage("Ciao")
        yield future
        self.assertEqual(future.result, 'Ciao')
        