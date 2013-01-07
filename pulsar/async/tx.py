'''Pulsar & twisted utilities'''
import twisted
from twisted.internet.posixbase import PosixReactorBase
from twisted.internet.defer import Deferred as TwistedDeferred

from .access import thread_ioloop


def wrap_deferred(d):
    if isinstance(d, TwistedDeferred) and not hasattr(d, 'add_both'):
        d.add_both = d.addBoth
    return d


class PulsarReactor(PosixReactorBase):
    '''A proxy for the a twisted reactor.'''
    
    def installWaker(self):
        pass
    
    def callLater(self, _seconds, _f, *args, **kw):
        ioloop = thread_ioloop()
        ioloop.call_later(_seconds, _f, *args, **kw)
    
    
pulsar_reactor = PulsarReactor()