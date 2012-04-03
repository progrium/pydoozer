import os
import struct

import gevent
import gevent.event
import gevent.socket
import google.protobuf.message

from msg_pb2 import Response
from msg_pb2 import Request

CONNECT_TIMEOUT = 5.0
REQUEST_TIMEOUT = 2.0
DEFAULT_URI = "doozer:?%s" % "&".join([
    "ca=127.0.0.1:8046",
    "ca=127.0.0.1:8041",
    "ca=127.0.0.1:8042",
    "ca=127.0.0.1:8043",
    ])

_spawner = gevent.spawn

class ConnectError(Exception): pass
class ResponseError(Exception):
    def __init__(self, response, request):
        self.code = response.err_code
        self.detail = response.err_detail
        self.response = response
        self.request = request
    
    def __str__(self):
        return str(pb_dict(self.request))

class TagInUse(ResponseError): pass
class UnknownVerb(ResponseError): pass
class Readonly(ResponseError): pass
class TooLate(ResponseError): pass
class RevMismatch(ResponseError): pass
class BadPath(ResponseError): pass
class MissingArg(ResponseError): pass
class Range(ResponseError): pass
class NotDirectory(ResponseError): pass
class IsDirectory(ResponseError): pass
class NoEntity(ResponseError): pass

def response_exception(response):
    """Takes a response, returns proper exception if it has an error code"""
    exceptions = {
        Response.TAG_IN_USE: TagInUse, Response.UNKNOWN_VERB: UnknownVerb,
        Response.READONLY: Readonly, Response.TOO_LATE: TooLate, 
        Response.REV_MISMATCH: RevMismatch, Response.BAD_PATH: BadPath,
        Response.MISSING_ARG: MissingArg, Response.RANGE: Range,
        Response.NOTDIR: NotDirectory, Response.ISDIR: IsDirectory,
        Response.NOENT: NoEntity, }
    if 'err_code' in [field.name for field, value in response.ListFields()]:
        return exceptions[response.err_code]
    else:
        return None

def pb_dict(message):
    """Create dict representation of a protobuf message"""
    return dict([(field.name, value) for field, value in message.ListFields()])

def parse_uri(uri):
    """Parse the doozerd URI scheme to get node addresses"""
    if uri.startswith("doozer:?"):
        before, params = uri.split("?", 1)
        addrs = []
        for param in params.split("&"):
            key, value = param.split("=", 1)
            if key == "ca":
                addrs.append(value)
        return addrs
    else:
        raise ValueError("invalid doozerd uri")

def connect(uri=None):
    """Start a Doozer client connection"""
    uri = uri or os.environ.get("DOOZER_URI", DEFAULT_URI)
    addrs = parse_uri(uri)
    if not addrs:
        raise ValueError("there were no addrs supplied in the uri (%s)" % uri)
    return Client(addrs)

class Connection(object):
    def __init__(self, addrs=None):
        if addrs is None:
            addrs = []
        self.addrs = addrs
        self.pending = {}
        self.loop = None
        self.sock = None
        self.address = None
        self.ready = gevent.event.Event()
    
    def connect(self):
        self.reconnect()
    
    def reconnect(self):
        self.disconnect()
        for retry in range(5):
            addrs = list(self.addrs)
            while len(addrs):
                try:
                    host, port = addrs.pop(0).split(':')
                    self.address = "%s:%s" % (host, port)
                    self.sock = gevent.socket.create_connection((host, int(port)))
                    self.ready.set()
                    self.loop = _spawner(self._recv_loop)
                    return
                except IOError:
                    pass
            gevent.sleep(retry * 2)
        raise ConnectError("Can't connect to any of the addresses: %s" % self.addrs)
    
    def disconnect(self):
        if self.loop:
            self.loop.kill()
        if self.sock:
            self.sock.close()
        self.ready.clear()
    
    def send(self, request, retry=True):
        request.tag = 0
        while request.tag in self.pending:
            request.tag += 1
            request.tag %= 2**31
        self.pending[request.tag] = gevent.event.AsyncResult()
        data = request.SerializeToString()
        head = struct.pack(">I", len(data))
        packet = ''.join([head, data])
        try:
            self.ready.wait(timeout=2)
            self.sock.send(packet)
        except IOError, e:
            self.reconnect()
            if retry:
                self.ready.wait()
                self.sock.send(packet)
            else:
                raise e
        response = self.pending[request.tag].get(timeout=REQUEST_TIMEOUT)
        del self.pending[request.tag]
        exception = response_exception(response)
        if exception:
            raise exception(response, request)
        return response
    
    def _recv_loop(self):
        while True:
            try:
                head = self.sock.recv(4)
                length = struct.unpack(">I", head)[0]
                data = self.sock.recv(length)
                response = Response()
                response.ParseFromString(data)
                if response.tag in self.pending:
                    self.pending[response.tag].set(response)
            except struct.error, e:
                # If some extra bytes are sent, just reconnect. 
                # This is related to this bug: 
                # https://github.com/ha/doozerd/issues/5
                self.reconnect()
            except IOError, e:
                self.reconnect()
                

class Client(object):
    def __init__(self, addrs=None):
        if addrs is None:
            addrs = []
        self.connection = Connection(addrs)
        self.connect()
    
    def rev(self):
        request = Request(verb=Request.REV)
        return self.connection.send(request)
        
    def set(self, path, value, rev):
        request = Request(path=path, value=value, rev=rev, verb=Request.SET)
        return self.connection.send(request, retry=False)
        
    def get(self, path, rev=None):
        request = Request(path=path, verb=Request.GET)
        if rev:
            request.rev = rev
        return self.connection.send(request)
    
    def delete(self, path, rev):
        request = Request(path=path, rev=rev, verb=Request.DEL)
        return self.connection.send(request, retry=False)
    
    def wait(self, path, rev):
        request = Request(path=path, rev=rev, verb=Request.WAIT)
        return self.connection.send(request)
    
    def stat(self, path, rev):
        request = Request(path=path, rev=rev, verb=Request.STAT)
        return self.connection.send(request)
    
    def access(self, secret):
        request = Request(value=secret, verb=Request.ACCESS)
        return self.connection.send(request)
    
    def _getdir(self, path, offset=0, rev=None):
        request = Request(path=path, offset=offset, verb=Request.GETDIR)
        if rev:
            request.rev = rev
        return self.connection.send(request)
        
    def _walk(self, path, offset=0, rev=None):
        request = Request(path=path, offset=offset, verb=Request.WALK)
        if rev:
            request.rev = rev
        return self.connection.send(request)
    
    def watch(self, path, rev):
        raise NotImplementedError()
    
    def _list(self, method, path, offset=None, rev=None):
        offset = offset or 0
        entities = []
        try:
            while True:
                response = getattr(self, method)(path, offset, rev)
                entities.append(response)
                offset += 1
        except ResponseError, e:
            if e.code == Response.RANGE:
                return entities
            else:
                raise e
            
    def walk(self, path, offset=None, rev=None):
        return self._list('_walk', path, offset, rev)
    
    def getdir(self, path, offset=None, rev=None):
        return self._list('_getdir', path, offset, rev)
    
    def disconnect(self):
        self.connection.disconnect()
    
    def connect(self):
        self.connection.connect()
