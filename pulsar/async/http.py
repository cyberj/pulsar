'''Asynchronous HTTP client'''
import pulsar
from pulsar import lib
from pulsar.utils import httpurl

from .iostream import AsyncIOStream


__all__ = ['HttpClient']
    


class AsyncRead(object):
    
    def async_read(self):
        return safe_async(self.keep_reading, max_errors=1).add_errback(self.close)
        
    def keep_reading(self):
        msg = None
        while not self.sock.closed:
            future = self.sock.read()
            yield future
            msg = self.parsedata(future.outcome)
            if msg is not None:
                break
        yield msg


class HttpResponse(AsyncRead, httpurl.HttpResponse):
    pass
    
    
class HttpConnection(httpurl.HttpConnection):
    
    def connect(self):
        if self.timeout == 0:
            self.sock = AsyncIOStream()
            c = self.sock.connect((self.host, self.port))
            if self._tunnel_host:
                c.add_callback(lambda r: self._tunnel())
        else:
            httpurl.HttpConnection.connect(self)
            
    @property
    def closed(self):
        if self.timeout == 0:
            if not self.sock.closed:
                return httpurl.is_closed(self.sock.sock)
            else:
                return True
        else:
            return httpurl.is_closed(self.sock)
        

class HttpClient(httpurl.HttpClient):
    timeout = 0
    client_version = pulsar.SERVER_SOFTWARE
    http_connection = HttpConnection
    response_class = HttpResponse
    