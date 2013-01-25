from time import time
import os
import sys
import signal
from multiprocessing import current_process

import pulsar
from pulsar.utils.tools import Pidfile
from pulsar.utils.security import gen_unique_id
from pulsar.utils.pep import itervalues, iteritems
from pulsar import HaltServer, system

from .actor import Actor, ACTOR_STATES
from .monitor import PoolMixin, _spawn_actor
from .defer import multi_async, log_failure
from .access import get_actor, set_actor
from .mailbox import MailboxServerConsumer
from . import proxy


process_global = pulsar.process_global

__all__ = ['arbiter', 'spawn', 'Arbiter']


def arbiter(commands_set=None, **params):
    '''Obtain the arbiter instance.'''
    arbiter = get_actor()
    if arbiter is None:
        # Create the arbiter
        return set_actor(_spawn_actor(Arbiter, None, **params))
    elif isinstance(arbiter, Actor) and arbiter.is_arbiter():
        return arbiter

#TODO: why cfg is set to None?
def spawn(cfg=None, **kwargs):
    '''Spawn a new :class:`Actor` and return an :class:`ActorProxyDeferred`.
This method can be used from any :class:`Actor`.
If not in the :class:`Arbiter` domain,
the method send a request to the :class:`Arbiter` to spawn a new actor. Once
the arbiter creates the actor it returns the proxy to the original caller.

**Parameter kwargs**

These optional parameters are:
    * *actor_class* a custom :class:`Actor` subclass.
    * *aid* the actor id
    * *commands_set* the set of :ref:`remote commands <api-remote_commands>`
      the :class:`Actor` can respond to.
    
:rtype: an :class:`ActorProxyDeferred`.

A typical usage::

    >>> a = spawn()
    >>> a.aid
    'ba42b02b'
    >>> a.called
    True
    >>> p = a.result
    >>> p.address
    ('127.0.0.1', 46691)
    '''
    aid = gen_unique_id()[:8]
    kwargs['aid'] = aid
    actor = get_actor()
    # The actor is not the Arbiter domain.
    # We send a message to the Arbiter to spawn a new Actor
    if not isinstance(actor, Arbiter):
        # send the request to the arbiter
        response = actor.send('arbiter', 'spawn', **kwargs)
        msg = response.when_ready.add_callback(actor.link_actor)
        return proxy.ActorProxyDeferred(aid, msg)
    else:
        return actor.spawn(**kwargs)


def stop_arbiter(self):
    p = self.pidfile
    if p is not None:
        self.logger.debug('Removing %s' % p.fname)
        p.unlink()
    if self.managed_actors:
        self.state = ACTOR_STATES.TERMINATE
    self.logger.info("Bye.")
    if self.exit_code:
        sys.exit(self.exit_code)
    
def start_arbiter(self):
    if current_process().daemon:
        raise pulsar.PulsarException(
                'Cannot create the arbiter in a daemon process')
    os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
    pidfile = self.cfg.pidfile
    if pidfile is not None:
        p = Pidfile(pidfile)
        p.create(self.pid)
        self.pidfile = p
    
def info_arbiter(args):
    self, data = args
    monitors = [p.info() for p in itervalues(self.monitors)]
    server = data.pop('actor')
    server.update({'version': pulsar.__version__,
                   'name': pulsar.SERVER_NAME,
                   'number_of_monitors': len(self.monitors),
                   'number_of_actors': len(self.managed_actors)})
    server.pop('is_process', None)
    server.pop('ppid', None)
    server.pop('actor_id', None)
    server.pop('age', None)
    data['server'] = server
    data['workers'] = [a.info for a in itervalues(self.managed_actors)]
    data['monitors'] = monitors
    return data
    
    
class Arbiter(PoolMixin):
    '''The Arbiter is the most important a :class:`Actor`
and :class:`PoolMixin` in pulsar concurrent framework. It is used as singleton
in the main process and it manages one or more :class:`Monitor`.
It runs the main :class:`EventLoop` of your concurrent application.
It is the equivalent of the gunicorn_ arbiter, the twisted_ reactor
and the tornado_ eventloop.

Users access the arbiter (in the arbiter process domain) by the high level api::

    import pulsar

    arbiter = pulsar.arbiter()

.. _gunicorn: http://gunicorn.org/
.. _twisted: http://twistedmatrix.com/trac/
.. _tornado: http://www.tornadoweb.org/
'''
    pidfile = None
    
    def __init__(self, impl):
        super(Arbiter, self).__init__(impl)
        self.monitors = {}
        self.registered = {'arbiter': self}
        self.bind_event('start', start_arbiter)
        self.bind_event('stop', stop_arbiter)

    ############################################################################
    # ARBITER HIGH LEVEL API
    ############################################################################
    def is_arbiter(self):
        return True

    def add_monitor(self, monitor_class, monitor_name, **params):
        '''Add a new :class:`Monitor` to the :class:`Arbiter`.

:parameter monitor_class: a :class:`pulsar.Monitor` class.
:parameter monitor_name: a unique name for the monitor.
:parameter kwargs: dictionary of key-valued parameters for the monitor.
:rtype: an instance of a :class:`pulsar.Monitor`.'''
        if monitor_name in self.registered: 
            raise KeyError('Monitor "{0}" already available'\
                           .format(monitor_name))
        params['name'] = monitor_name
        m = self.spawn(monitor_class, **params)
        self.registered[m.name] = m
        self.monitors[m.aid] = m
        return m

    def is_process(self):
        return True

    def close_monitors(self):
        '''Close all :class:`Monitor` at once.'''
        return multi_async([m.stop() for m in list(itervalues(self.monitors))],
                           log_failure=True)

    def get_actor(self, aid):
        '''Given an actor unique id return the actor proxy.'''
        a = super(Arbiter, self).get_actor(aid)
        if a is None:
            if aid in self.monitors:
                return self.monitors[aid]
            else:
                return self.registered.get(aid)
    
    ############################################################################
    # INTERNALS
    ############################################################################
    def _remove_actor(self, actor, log=True):
        super(Arbiter, self)._remove_actor(actor, log)
        self.registered.pop(actor.name, None)
        self.monitors.pop(actor.aid, None)
        
    def periodic_task(self):
        # Arbiter periodic task
        if self.can_continue() and self.running():
            # managed actors job
            self.manage_actors()
            for m in list(itervalues(self.monitors)):
                if m.started():
                    if not m.running():
                        self._remove_actor(m)
                else:
                    m.start()
            try:
                self.cfg.arbiter_task(self)
            except Exception:
                pass
        # requestloop and ioloop are the same. Use requestloop because ioloop
        # is not available at startup
        self.requestloop.call_soon(self.periodic_task)

    def _stop(self):
        '''Stop the pools the message queue and remaining actors.'''
        self.requestloop.call_soon_threadsafe(self._exit)
        self.requestloop.run()
        
    def _exit(self, res=None):
        if res:
            self.state = ACTOR_STATES.CLOSE
            self.mailbox.close()
        else:
            active = multi_async((self.close_monitors(), self.close_actors()),
                                 log_failure=True)
            active.add_both(self._exit)
                
    def start(self):
        if self.state == ACTOR_STATES.INITIAL:
            if self.cfg.daemon: #pragma    nocover
                system.daemonize()
            return Actor.start(self)
        
    def _mailbox(self):
        #if platform.type == 'posix':
        #    address = 'unix:%s.pulsar' % actor.aid
        #else:   #pragma    nocover
        #    address = ('127.0.0.1', 0)
        address = ('127.0.0.1', 0)
        mailbox = self.requestloop.create_server(address=address,
                                        name='Mailbox for %s' % self,
                                        consumer_factory=MailboxServerConsumer,
                                        timeout=0,
                                        close_event_loop=True)
        mailbox.event_loop.call_soon_threadsafe(self.hand_shake)
        return mailbox