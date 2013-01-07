'''A simple webmail client. It requires twisted.'''
import twisted
from twisted.internet import protocol, defer, endpoints, task
from twisted.mail import imap4

try:
    import pulsar
except ImportError: #pragma    nocover
    sys.path.append('../../')
    import pulsar

from pulsar.apps import wsgi
from pulsar.async.tx import pulsar_reactor


class imap(pulsar.Setting):
    flags = ['--imap']
    default = 'ssl:host=smtp.gmail.com:port=993'
    desc = 'IMAP client'


def email_client(strport):
    endpoint = endpoints.clientFromString(pulsar_reactor, strport)
    factory = protocol.Factory()
    factory.protocol = imap4.IMAP4Client
    return endpoint.connect(factory)
    
    
class WebMailMiddleware:
    '''WSGI application running on the server'''
    def __call__(self, environ, start_response):
        response = self.request(environ)
        return response(environ, start_response)
    
    def login(self, environ):
        pass
    
    
def server(description=None, **kwargs):
    description = description or 'Pulsar Webmail'
    return wsgi.WSGIServer(callable=WebMailMiddleware(),
                           description=description,
                           **kwargs)
    

if __name__ == '__main__':  #pragma nocover
    server().start()