'''Pulsar & twisted utilities'''
import twisted
from twisted.internet import reactor
from twisted.internet.defer import Deferred as TwistedDeferred


def wrap_deferred(d):
    if isinstance(d, TwistedDeferred) and not hasattr(d, 'add_both'):
        d.add_both = d.addBoth
    return d


class pulsar_reactor:
    '''A proxy for the default twisted reactor.'''
    
    def callLater(self, _seconds, _f, *args, **kw):
        pass
    
    def __getattr__(self, attrname):
        return getattr(reactor, attrname)
    
    
pulsar_reactor = pulsar_reactor()