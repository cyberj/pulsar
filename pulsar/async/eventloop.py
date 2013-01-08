import os
import sys
import heapq
import logging
import traceback
import signal
import errno
import socket
import time
from threading import current_thread

from pulsar import Timeout, AlreadyCancelled, AlreadyCalledError
from pulsar.utils.system import IObase, IOpoll, close_on_exec, platform, Waker
from pulsar.utils.security import gen_unique_id
from pulsar.utils.log import Synchronized
from pulsar.utils.structures import WeakList

from .defer import Deferred, is_async, maybe_async, make_async,\
                    log_failure, EXIT_EXCEPTIONS

__all__ = ['IOLoop', 'PeriodicCallback', 'TimedCall']

LOGGER = logging.getLogger('pulsar.eventloop')

if sys.version_info >= (3, 3):
    timer = time.monotonic
else:   #pragma    nocover
    timer = time.time
    
def file_descriptor(fd):
    if hasattr(fd, 'fileno'):
        return fd.fileno()
    else:
        return fd

def setid(self):
    self.tid = current_thread().ident
    self.pid = os.getpid()


class TimedCall(object):
    """An IOLoop timeout, a UNIX timestamp and a callback"""

    def __init__(self, deadline, callback, args, canceller):
        self.deadline = deadline
        self.canceller = canceller
        self._callback = callback
        self._args = args
        self._cancelled = self._called = False

    def __lt__(self, other):
        return self.deadline < other.deadline
        
    @property
    def cancelled(self):
        return self._cancelled
    
    @property
    def called(self):
        return self._called
    
    @property
    def callback(self):
        return self._callback
    
    @property
    def args(self):
        return self._args
    
    def cancel(self):
        '''Attempt to cancel the callback.'''
        if self.cancelled:
            raise AlreadyCancelled()
        elif self.called:
            raise AlreadyCalledError()
        else:
            self.canceller(self)
            self._cancelled = True

    def __call__(self):
        self._called = True
        self._callback(*self._args)
        

class LoopGuard(object):
    '''Context manager for the eventloop'''
    def __init__(self, loop):
        self.loop = loop

    def __enter__(self):
        loop = self.loop
        loop.logger.subdebug("Starting event loop")
        loop._running = True
        if not loop._started:
            loop._started = timer()
        setid(loop)
        loop._on_exit = Deferred(description='IOloop.on_exit')
        return self

    def __exit__(self, type, value, traceback):
        loop = self.loop
        loop._running = False
        loop.logger.subdebug('Exiting event loop')
        loop._on_exit.callback(loop)

pass_through = lambda: None

class FileDescriptor(IObase):
    def __init__(self, fd, eventloop, read=None, write=None, connect=None,
                 error=None):
        self.fd = fd
        self.eventloop = eventloop
        self.state = None
        self._connecting = False
        self.handle_write = None
        self.handle_read = None
        if connect:
            self.add_connector(connect)
        elif write:
            self.add_writer(write)
        if read:
            self.add_reader(read)
        self.handle_error = error or pass_through

    @property
    def poller(self):
        return self.eventloop._impl
    
    @property
    def reading(self):
        return self.state & self.READ
    
    @property
    def writing(self):
        return self.state & self.WRITE
    
    @property
    def connecting(self):
        return self._connecting
    
    @property
    def state_code(self):
        s = []
        if self.state is None:
            return 'closed'
        if self.connecting:
            s.append('connecting')
        elif self.writing:
            s.append('writing')
        if self.reading:
            s.append('reading')
        return ' '.join(s) if s else 'idle'
    
    def add_connector(self, callback):
        if self.state is not None:
            raise RuntimeError('Cannot connect. State is %s.' % self.state_code)
        self._connecting = True
        self.add_writer(callback)
        
    def add_reader(self, callback):
        if not self.handle_read:
            state = self.state or self.ERROR
            self.handle_read = callback
            self.modify_state(state | self.READ)
        else:
            raise RuntimeError("Asynchronous stream already reading!")
        
    def add_writer(self, callback):
        if not self.handle_write:
            state = self.state or self.ERROR
            self.handle_write = callback
            self.modify_state(state | self.WRITE)
        else:
            raise RuntimeError("Asynchronous stream already writing!")
        
    def remove_connector(self):
        self._connecting = False
        return self.remove_writer()
    
    def remove_reader(self):
        '''Remove reader and return True if writing'''
        state = self.ERROR
        if self.writing:
            state |= self.WRITE
        self.handle_read = None
        self.modify_state(state)
        return self.writing

    def remove_writer(self):
        '''Remove writer and return True if reading'''
        state = self.ERROR
        if self.reading:
            state |= self.READ
        self.handle_write = None
        self.modify_state(state)
        return self.reading
    
    def __call__(self, fd, events):
        if events & self.READ:
            self.handle_read()
        if events & self.WRITE:
            self.handle_write()
        if events & self.ERROR:
            self.handle_error()
            
    def modify_state(self, state):
        if self.state != state:
            if self.state is None:
                self.state = state
                self.poller.register(self.fd, state)
            else:
                self.state = state
                self.poller.modify(self.fd, state)
        
        
class IOLoop(IObase, Synchronized):
    """A level-triggered I/O event loop adapted from tornado to
conform with pep-3156.

:parameter io: The I/O implementation. If not supplied, the best possible
    implementation available will be used. On posix system this is ``epoll``,
    or else ``select``. It can be any other custom implementation as long as
    it has an ``epoll`` like interface. Pulsar ships with an additional
    I/O implementation based on distributed queue :class:`IOQueue`.

**ATTRIBUTES**

.. attribute:: _impl

    The IO implementation

.. attribute:: cpubound

    If ``True`` this is a CPU bound event loop, otherwise it is an I/O
    event loop. CPU bound loops can block the loop for considerable amount
    of time.
        
.. attribute:: num_loops

    Total number of loops

.. attribute:: poll_timeout

    The timeout in seconds when polling with epol or select.

    Default: `0.5`

.. attribute:: tid

    The thread id where the eventloop is running
    
.. attribute:: tasks

    A list of callables to be executed at each iteration of the event loop.
    Task can be added and deleted via the :meth:`add_task` and
    :meth:`remove_task`. Extra care must be taken when adding tasks to
    I/O event loops. These tasks should be fast to perform and not block.

**METHODS**
"""
    # Never use an infinite timeout here - it can stall epoll
    poll_timeout = 0.5

    def __init__(self, io=None, logger=None, poll_timeout=None):
        self._impl = io or IOpoll()
        self.fd_factory = getattr(self._impl, 'fd_factory', FileDescriptor)
        self.poll_timeout = poll_timeout if poll_timeout else self.poll_timeout
        self.logger = logger or LOGGER
        if hasattr(self._impl, 'fileno'):
            close_on_exec(self._impl.fileno())
        self._handlers = {}
        self._events = {}
        self._callbacks = []
        self._scheduled = []
        self._state = None
        self._started = None
        self._running = False
        self.num_loops = 0
        self._waker = getattr(self._impl, 'waker', Waker)()
        self._on_exit = None
        self.add_reader(self._waker, self._waker.consume)

    @property
    def cpubound(self):
        return getattr(self._impl, 'cpubound', False)

    def remove_handler(self, fd):
        """Stop listening for events on fd."""
        fdd = file_descriptor(fd)
        self._handlers.pop(fdd, None)
        self._events.pop(fdd, None)
        try:
            self._impl.unregister(fdd)
        except (OSError, IOError):
            self.logger.error("Error removing %s from IOLoop", fd, exc_info=True)

    def start(self):
        if self._running:
            return False
        self._run()

    def stop(self):
        '''Stop the loop after the current event loop iteration is complete.
If the event loop is not currently running, the next call to :meth:`start`
will return immediately.

To use asynchronous methods from otherwise-synchronous code (such as
unit tests), you can start and stop the event loop like this::

    ioloop = IOLoop()
    async_method(ioloop=ioloop, callback=ioloop.stop)
    ioloop.start()

:meth:`start` will return after async_method has run its callback,
whether that callback was invoked before or after ioloop.start.'''
        if self.running():
            self._running = False
            self.wake()
        return self._on_exit

    def running(self):
        """Returns true if this IOLoop is currently running."""
        return self._running

    def call_later(self, seconds, callback, *args):
        """Add a *callback* to be executed approximately *seconds* in the
future, once, unless cancelled. A timeout callback  it is called
at the time *deadline* from the :class:`IOLoop`.
It returns an handle that may be passed to remove_timeout to cancel."""
        if seconds > 0:
            timeout = TimedCall(timer() + seconds, callback, args,
                                self.remove_timeout)
            heapq.heappush(self._scheduled, timeout)
            return timeout
        else:
            return self.call_soon(callback, *args)

    def call_soon(self, callback, *args):
        '''Equivalent to ``self.call_later(0, callback, *args, **kw)``.'''
        timeout = TimedCall(None, callback, args, self.remove_timeout)
        self._callbacks.append(timeout)
        return timeout
    
    def call_soon_threadsafe(self, callback, *args):
        '''Equivalent to ``self.call_later(0, callback, *args, **kw)``.'''
        timeout = self.call_soon(callback, *args)
        self.wake()
        return timeout

    # METHODS FOR REGISTERING CALLBACKS ON FILE DESCRIPTORS
    def add_connector(self, fd, callback):
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].add_connector(callback)
        else:
            self._handlers[fd] = self.fd_factory(fd, self, connect=callback)
            
    def add_reader(self, fd, callback):
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].add_reader(callback)
        else:
            self._handlers[fd] = self.fd_factory(fd, self, read=callback)
        
    def add_writer(self, fd, callback):
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].add_writer(callback)
        else:
            self._handlers[fd] = self.fd_factory(fd, self, write=callback)
        
    def add_error_handler(self, fd, callback):
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].handle_error = callback
            
    def remove_connector(self, fd):
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].remove_connector()
        
    def remove_reader(self, fd):
        '''Cancels the current read callback for file descriptor fd,
if one is set. A no-op if no callback is currently set for the file
descriptor.'''
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].remove_reader()
    
    def remove_writer(self, fd):
        '''Cancels the current write callback for file descriptor fd,
if one is set. A no-op if no callback is currently set for the file
descriptor.'''
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].remove_writer()
        
    def add_periodic(self, callback, period):
        """Add a :class:`PeriodicCallback` to the event loop."""
        p = PeriodicCallback(callback, period, self)
        p.start()
        return p
        
    def wake(self):
        '''Wake up the eventloop.'''
        if self.running():
            self._waker.wake()
            
    def remove_timeout(self, timeout):
        """Cancels a pending *timeout*. The argument is an handle as returned
by the :meth:`add_timeout` method."""
        self._scheduled.remove(timeout)

    ############################################################ INTERNALS
    def _run_callback(self, callback):
        try:
            callback()
        except EXIT_EXCEPTIONS:
            raise
        except:
            self.logger.critical('Exception in callback.', exc_info=True)

    def _run(self):
        """Runs the I/O loop until one of the I/O handlers calls stop(), which
will make the loop stop after the current event iteration completes."""
        with LoopGuard(self) as guard:
            while self._running:
                poll_timeout = self.poll_timeout
                self.num_loops += 1
                _run_callback = self._run_callback
                # Prevent IO event starvation by delaying new callbacks
                # to the next iteration of the event loop.
                callbacks = self._callbacks
                if callbacks:
                    self._callbacks = []
                    for callback in callbacks:
                        _run_callback(callback)
                if self._scheduled:
                    now = timer()
                    while self._scheduled and self._scheduled[0].deadline <= now:
                        timeout = self._scheduled.pop(0)
                        self._run_callback(timeout)
                    if self._scheduled:
                        milliseconds = self._scheduled[0].deadline - now
                        poll_timeout = min(milliseconds, poll_timeout)
                # A chance to exit
                if not self._running:
                    break
                try:
                    event_pairs = self._impl.poll(poll_timeout)
                except Exception as e:
                    # Depending on python version and IOLoop implementation,
                    # different exception types may be thrown and there are
                    # two ways EINTR might be signaled:
                    # * e.errno == errno.EINTR
                    # * e.args is like (errno.EINTR, 'Interrupted system call')
                    eno = getattr(e, 'errno', None)
                    if eno != errno.EINTR:
                        args = getattr(e, 'args', None)
                        if isinstance(args, tuple) and len(args) == 2:
                            eno = args[0]
                    if eno != errno.EINTR and self._running:
                        raise
                    continue
                # Pop one fd at a time from the set of pending fds and run
                # its handler. Since that handler may perform actions on
                # other file descriptors, there may be reentrant calls to
                # this IOLoop that update self._events
                if event_pairs:
                    self._events.update(event_pairs)
                    _events = self._events
                    while _events:
                        fd, events = _events.popitem()
                        try:
                            self._handlers[fd](fd, events)
                        except EXIT_EXCEPTIONS:
                            raise
                        except (OSError, IOError) as e:
                            if e.args[0] == errno.EPIPE:
                                # Happens when the client closes the connection
                                pass
                            else:
                                self.logger.error(
                                    "Exception in I/O handler for fd %s",
                                              fd, exc_info=True)
                        except KeyError:
                            self.logger.info("File descriptor %s missing", fd)
                        except:
                            self.logger.error("Exception in I/O handler for fd %s",
                                          fd, exc_info=True)


class PeriodicCallback(object):
    """Schedules the given callback to be called periodically.

    The callback is called every callback_time seconds.
    """
    def __init__(self, callback, callback_time, ioloop):
        self.callback = callback
        self.callback_time = callback_time
        self.ioloop = ioloop
        self._running = False

    def start(self):
        self._running = True
        self.ioloop.call_later(self.callback_time, self._run)

    def stop(self):
        self._running = False

    def _run(self):
        if not self._running:
            return
        try:
            self.callback(self)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            LOGGER.error("Error in periodic callback", exc_info=True)
        if self._running:
            self.start()

