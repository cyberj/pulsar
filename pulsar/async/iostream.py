import sys
import io
import time
import logging
import socket
import errno
from collections import deque

from pulsar.utils.system import IObase
from pulsar import create_socket, server_socket, create_client_socket,\
                     wrap_socket, defaults, create_connection, CouldNotParse,\
                     get_socket_timeout, Timeout, BaseSocket
from pulsar.utils.structures import merge_prefix
from .defer import Deferred, is_async, is_failure, async, maybe_async,\
                        safe_async, log_failure, NOT_DONE, range
from .access import get_event_loop

LOGGER = logging.getLogger('pulsar.iostream')


__all__ = ['AsyncIOStream', 'WRITE_BUFFER_MAX_SIZE', 'loop_timeout']

WRITE_BUFFER_MAX_SIZE = 128 * 1024  # 128 kb

ASYNC_ERRNO = (errno.EWOULDBLOCK, errno.EAGAIN, errno.EINPROGRESS)
def async_error(e):
    return e.args and e.args[0] in ASYNC_ERRNO
    
def _not_called_exception(value):
    if not value.called:
        try:
            raise Timeout('"%s" timed out.' % value)
        except:
            value.callback(sys.exc_info())

def loop_timeout(value, timeout, ioloop=None):
    value = maybe_async(value)
    if timeout and is_async(value):
        ioloop = ioloop or get_event_loop()
        return ioloop.call_later(timeout, _not_called_exception, value)
    

class AsyncIOStream(IObase, BaseSocket):
    ''':ref:`Framework class <pulsar_framework>` to write and read
from a non-blocking socket. It is used everywhere in :mod:`pulsar` for
handling asynchronous :meth:`write` and :meth:`read` operations with
`callbacks` which can be used to act when data has just been sent or has
just been received.

It was originally forked from tornado_ IOStream and subsequently
adapted to pulsar :ref:`concurrent framework <design>`.

.. attribute:: socket

    A :class:`Socket` which might be connected or unconnected.

.. attribute:: timeout

    A timeout in second which is used when waiting for a
    data to be available for reading. If timeout is a positive number,
    every time the :class:`AsyncIOStream` performs a :meth:`read`
    operation a timeout is also created on the :attr:`ioloop`.
'''
    _error = None
    _socket = None
    _state = None
    _read_timeout = None
    _read_callback = None
    _read_length = None
    _write_callback = None
    _close_callback = None
    _connect_callback = None

    def __init__(self, socket=None, max_buffer_size=None,
                 read_chunk_size=None, timeout=None):
        self.sock = socket
        self._read_callback_timeout = timeout
        self.max_buffer_size = max_buffer_size or 104857600
        self.read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self._read_buffer = deque()
        self._write_buffer = deque()

    def __repr__(self):
        if self.sock:
            return '%s (%s)' % (self.sock, self.state_code)
        else:
            return '(closed)'

    def __str__(self):
        return self.__repr__()

    #######################################################    STATES
    @property
    def connecting(self):
        return self._connect_callback is not None

    @property
    def reading(self):
        """Returns true if we are currently reading from the stream."""
        return self._read_callback is not None

    @property
    def writing(self):
        """Returns true if we are currently writing to the stream."""
        return bool(self._write_buffer)

    @property
    def closed(self):
        '''Boolean indicating if the :attr:`sock` is closed.'''
        return self.sock is None

    @property
    def state(self):
        return self._state
    
    @property
    def error(self):
        return self._error

    @property
    def state_code(self):
        s = []
        if self.closed:
            return 'closed'
        if self.connecting:
            s.append('connecting')
        if self.writing:
            s.append('writing')
        if self.reading:
            s.append('reading')
        return ' '.join(s) if s else 'idle'

    @property
    def ioloop(self):
        return get_event_loop()
    
    def settimeout(self, value):
        pass

    def _set_socket(self, sock):
        if self._socket is None:
            self._socket = wrap_socket(sock)
            self._state = None
            if self._socket is not None:
                self._socket.settimeout(0)
        else:
            raise RuntimeError('Cannot set socket. Close the existing one.')
    def _get_socket(self):
        return self._socket
    sock = property(_get_socket, _set_socket)

    #######################################################    ACTIONS
    def connect(self, address):
        """Connects the socket to a remote address without blocking.
May only be called if the socket passed to the constructor was not available
or it was not previously connected.  The address parameter is in the
same format as for socket.connect, i.e. a (host, port) tuple or a string
for unix sockets.
If callback is specified, it will be called when the connection is completed.
Note that it is safe to call IOStream.write while the
connection is pending, in which case the data will be written
as soon as the connection is ready.  Calling IOStream read
methods before the socket is connected works on some platforms
but is non-portable."""
        if self._state is None and not self.connecting:
            if self.sock is None:
                self.sock = create_client_socket(address)
            try:
                self.sock.connect(address)
            except socket.error as e:
                # In non-blocking mode connect() always raises an exception
                if not async_error(e):
                    LOGGER.warning('Connect error on %s: %s', self, e)
                    self.close()
                    return
            callback = Deferred(description='%s connect callback' % self)
            self._connect_callback = callback
            self._add_io_state(self.WRITE)
            return callback
        else:
            raise RuntimeError('Cannot connect. State is %s.' % self.state_code)

    def read(self, length=None):
        """Starts reading data from the :attr:`sock`. It returns a
:class:`Deferred` which will be called back once data is available.
If this function is called while this class:`AsyncIOStream` is already reading
a RuntimeError occurs.

:rtype: a :class:`pulsar.Deferred` instance.

One common pattern of usage::

    def parse(data):
        ...

    io = AsyncIOStream(socket=sock)
    io.read().add_callback(parse)

"""
        if self.reading:
            raise RuntimeError("Asynchronous stream %s already reading!" %
                               str(self.address))
        if self.closed:
            return self._get_buffer(self._read_buffer)
        else:
            callback = Deferred(description='%s read callback' % self)
            self._read_callback = callback
            self._read_length = length
            self._add_io_state(self.READ)
            if self._read_timeout:
                try:
                    self.ioloop.remove_timeout(self._read_timeout)
                except ValueError:
                    pass
            self._read_timeout = loop_timeout(callback,
                                              self._read_callback_timeout,
                                              self.ioloop)
            return callback
    recv = read

    def write(self, data):
        """Write the given *data* to this stream. If there was previously
buffered write data and an old write callback, that callback is simply
overwritten with this new callback.

:rtype: a :class:`Deferred` instance or the number of bytes written.
        """
        self._check_closed()
        if data:
            assert isinstance(data, bytes)
            if len(data) > WRITE_BUFFER_MAX_SIZE:
                for i in range(0, len(data), WRITE_BUFFER_MAX_SIZE):
                    self._write_buffer.append(data[i:i+WRITE_BUFFER_MAX_SIZE])
            else:
                self._write_buffer.append(data)
        #
        if not self.connecting:
            tot_bytes = self._handle_write()
            # data still in the buffer
            if self._write_buffer:
                callback = Deferred(description='%s write callback' % self)
                self._write_callback = callback
                self._add_io_state(self.WRITE)
                return callback
            else:
                return tot_bytes
    sendall = write

    def close(self):
        """Close the :attr:`sock` and call the *callback* if it was
setup using the :meth:`set_close_callback` method."""
        if not self.closed:
            exc_info = sys.exc_info()
            if any(exc_info):
                self._error = exc_info[1]
            if self._state is not None:
                self.ioloop.remove_handler(self.fileno())
            self.sock.close()
            self._socket = None
            if self._close_callback:
                self._may_run_callback(self._close_callback)

    def set_close_callback(self, callback):
        """Call the given callback when the stream is closed."""
        self._close_callback = callback

    #######################################################    INTERNALS
    def read_to_buffer(self):
        #Reads from the socket and appends the result to the read buffer.
        #Returns the number of bytes read.
        length = self._read_length or self.read_chunk_size
        # Read from the socket until we get EWOULDBLOCK or equivalent.
        # SSL sockets do some internal buffering, and if the data is
        # sitting in the SSL object's buffer select() and friends
        # can't see it; the only way to find out if it's there is to
        # try to read it.
        while True:
            try:
                chunk = self.sock.recv(length)
            except socket.error as e:
                if async_error(e):
                    chunk = None
                else:
                    raise
            if not chunk:
                # if chunk is b'' close the socket
                if chunk is not None:
                    self.close()
                break
            self._read_buffer.append(chunk)
            if self._read_buffer_size() >= self.max_buffer_size:
                LOGGER.error("Reached maximum read buffer size")
                self.close()
                raise IOError("Reached maximum read buffer size")
            if len(chunk) < length:
                break
        return self._read_buffer_size()

    def _read_buffer_size(self):
        return sum(len(chunk) for chunk in self._read_buffer)

    def _may_run_callback(self, c, result=None):
        try:
            # Make sure that any uncaught error is logged
            log_failure(c.callback(result))
        except:
            # Close the socket on an uncaught exception from a user callback
            # (It would eventually get closed when the socket object is
            # gc'd, but we don't want to rely on gc happening before we
            # run out of file descriptors)
            self.close()

    def _handle_connect(self):
        callback = self._connect_callback
        self._connect_callback = None
        self._may_run_callback(callback)

    def _handle_read(self):
        try:
            try:
                result = self.read_to_buffer()
            except (socket.error, IOError, OSError) as e:
                if e.args and e.args[0] == errno.ECONNRESET:
                    # Treat ECONNRESET as a connection close rather than
                    # an error to minimize log spam  (the exception will
                    # be available on self.error for apps that care).
                    result = 0
                else:
                    raise
        except Exception:
            result = 0
            LOGGER.warning("Read error on %s.", self, exc_info=True)
        if result == 0:
            self.close()
        buffer = self._get_buffer(self._read_buffer)
        if self.reading:
            callback = self._read_callback
            self._read_callback = None
            self._may_run_callback(callback, buffer)

    def _handle_write(self):
        # keep count how many bytes we write
        tot_bytes = 0
        while self._write_buffer:
            try:
                sent = self.sock.send(self._write_buffer[0])
                if sent == 0:
                    # With OpenSSL, after send returns EWOULDBLOCK,
                    # the very same string object must be used on the
                    # next call to send.  Therefore we suppress
                    # merging the write buffer after an EWOULDBLOCK.
                    # A cleaner solution would be to set
                    # SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER, but this is
                    # not yet accessible from python
                    # (http://bugs.python.org/issue8240)
                    break
                merge_prefix(self._write_buffer, sent)
                self._write_buffer.popleft()
                tot_bytes += sent
            except socket.error as e:
                if async_error(e):
                    break
                else:
                    LOGGER.warning("Write error on %s: %s", self, e)
                    self.close()
                    return
        if not self._write_buffer and self._write_callback:
            callback = self._write_callback
            self._write_callback = None
            self._may_run_callback(callback, tot_bytes)
        return tot_bytes

    def _check_closed(self):
        if not self.sock:
            raise IOError("Stream is closed")

    def _get_buffer(self, dq):
        buff = b''.join(dq)
        dq.clear()
        return buff

    def _handle_events(self, fd, events):
        # This is the actual callback from the event loop
        if not self.sock:
            LOGGER.warning("Got events for closed stream %d", fd)
            return
        try:
            if events & self.READ:
                self._handle_read()
            if not self.sock:
                return
            if events & self.WRITE:
                if self.connecting:
                    self._handle_connect()
                self._handle_write()
            if not self.sock:
                return
            if events & self.ERROR:
                # We may have queued up a user callback in _handle_read or
                # _handle_write, so don't close the IOStream until those
                # callbacks have had a chance to run.
                self.ioloop.call_soon(self.close)
                return
            state = self.ERROR
            if self.reading:
                state |= self.READ
            if self.writing:
                state |= self.WRITE
            if state != self._state:
                assert self._state is not None, \
                    "shouldn't happen: _handle_events without self._state"
                self._state = state
                self.ioloop.update_handler(self.fileno(), self._state)
        except:
            self.close()
            raise

    def _add_io_state(self, state):
        if self.sock is None:
            # connection has been closed, so there can be no future events
            return
        if self._state is None:
            # If the state was not set add the handler to the event loop
            self._state = self.ERROR | state
            self.ioloop.add_handler(self, self._handle_events, self._state)
        elif not self._state & state:
            # update the handler
            self._state = self._state | state
            self.ioloop.update_handler(self, self._state)

