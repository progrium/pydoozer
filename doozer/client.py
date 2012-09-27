import logging
import os
import random
import struct

import gevent
import gevent.event
import gevent.socket

from msg_pb2 import Response
from msg_pb2 import Request

REQUEST_TIMEOUT = 2.0

DEFAULT_RETRY_WAIT = 2.0
"""Default connection retry waiting time (seconds)"""

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


def connect(uri=None, timeout=None):
    """
    Start a Doozer client connection

    @param uri: str|None, Doozer URI
    @param timeout: float|None, connection timeout in seconds (per address)
    """

    uri = uri or os.environ.get("DOOZER_URI", DEFAULT_URI)
    addrs = parse_uri(uri)
    if not addrs:
        raise ValueError("there were no addrs supplied in the uri (%s)" % uri)
    return Client(addrs, timeout)


class Connection(object):
    def __init__(self, addrs=None, timeout=None):
        """
        @param timeout: float|None, connection timeout in seconds (per address)
        """
        self._logger = logging.getLogger('pydoozer.Connection')
        self._logger.debug('__init__(%s)', addrs)

        if addrs is None:
            addrs = []
        self.addrs = addrs
        self.addrs_index = 0
        """Next address to connect to in self.addrs"""

        self.pending = {}
        self.loop = None
        self.sock = None
        self.address = None
        self.timeout = timeout
        self.ready = gevent.event.Event()

        # Shuffle the addresses so all clients don't connect to the
        # same node in the cluster.
        random.shuffle(addrs)

    def connect(self):
        self.reconnect()

    def reconnect(self, kill_loop=True):
        """
        Reconnect to the cluster.

        @param kill_loop: bool, kill the current receive loop
        """

        self._logger.debug('reconnect()')

        self.disconnect(kill_loop)

        # Default to the socket timeout
        retry_wait = self.timeout or gevent.socket.getdefaulttimeout() or DEFAULT_RETRY_WAIT
        for retry in range(5):
            addrs_left = len(self.addrs)
            while addrs_left:
                try:
                    parts = self.addrs[self.addrs_index].split(':')
                    self.addrs_index = (self.addrs_index + 1) % len(self.addrs)
                    host = parts[0]
                    port = parts[1] if len(parts) > 1 else 8046
                    self.address = "%s:%s" % (host, port)
                    self._logger.debug('Connecting to %s...', self.address)
                    self.sock = gevent.socket.create_connection((host, int(port)),
                                                                timeout=self.timeout)
                    self._logger.debug('Connection successful')

                    # Reset the timeout on the connection so it
                    # doesn't make .recv() and .send() timeout.
                    self.sock.settimeout(None)
                    self.ready.set()

                    # Any commands that were in transit when the
                    # connection was lost is obviously not getting a
                    # reply. Retransmit them.
                    self._retransmit_pending()
                    self.loop = _spawner(self._recv_loop)
                    return

                except IOError, e:
                    self._logger.info('Failed to connect to %s (%s)', self.address, e)
                    pass
                addrs_left -= 1

            self._logger.debug('Waiting %d seconds to reconnect', retry_wait)
            gevent.sleep(retry_wait)
            retry_wait *= 2

        self._logger.error('Could not connect to any of the defined addresses')
        raise ConnectError("Can't connect to any of the addresses: %s" % self.addrs)

    def disconnect(self, kill_loop=True):
        """
        Disconnect current connection.

        @param kill_loop: bool, Kill the current receive loop
        """
        self._logger.debug('disconnect()')

        if kill_loop and self.loop:
            self._logger.debug('killing loop')
            self.loop.kill()
            self.loop = None
        if self.sock:
            self._logger.debug('closing connection')
            self.sock.close()
            self.sock = None

        self._logger.debug('clearing ready signal')
        self.ready.clear()
        self.address = None

    def send(self, request, retry=True):
        request.tag = 0
        while request.tag in self.pending:
            request.tag += 1
            request.tag %= 2**31

        # Create and send request
        data = request.SerializeToString()
        data_len = len(data)
        head = struct.pack(">I", data_len)
        packet = ''.join([head, data])
        entry = self.pending[request.tag] = {
            'event': gevent.event.AsyncResult(),
            'packet': packet,
        }
        self._logger.debug('Sending packet, tag: %d, len: %d', request.tag, data_len)
        self._send_pack(packet, retry)

        # Wait for response
        response = entry['event'].get(timeout=REQUEST_TIMEOUT)
        del self.pending[request.tag]
        exception = response_exception(response)
        if exception:
            raise exception(response, request)
        return response

    def _send_pack(self, packet, retry=True):
        """
        Send the given packet to the currently connected node.

        @param packet: struct, packet to send
        @param retry: bool, retry the sending once
        """
        try:
            self.ready.wait(timeout=2)
            self.sock.send(packet)
        except IOError, e:
            self._logger.warning('Error sending packet (%s)', e)
            self.reconnect()
            if retry:
                self._logger.debug('Retrying sending packet')
                self.ready.wait()
                self.sock.send(packet)
            else:
                self._logger.warning('Failed retrying to send packet')
                raise e

    def _recv_loop(self):
        self._logger.debug('_recv_loop(%s)', self.address)

        while True:
            try:
                head = self.sock.recv(4)
                length = struct.unpack(">I", head)[0]
                data = self.sock.recv(length)
                response = Response()
                response.ParseFromString(data)
                self._logger.debug('Received packet, tag: %d, len: %d', response.tag, length)
                if response.tag in self.pending:
                    self.pending[response.tag]['event'].set(response)
            except struct.error, e:
                self._logger.warning('Got invalid packet from server (%s)', e)
                # If some extra bytes are sent, just reconnect. 
                # This is related to this bug: 
                # https://github.com/ha/doozerd/issues/5
                break
            except IOError, e:
                self._logger.warning('Lost connection? (%s)', e)
                break

        # Note: .reconnect() will spawn a new loop
        self.reconnect(kill_loop=False)

    def _retransmit_pending(self):
        """
        Retransmits all pending packets.
        """

        for i in xrange(0, len(self.pending)):
            self._logger.debug('Retransmitting packet')
            try:
                self._send_pack(self.pending[i]['packet'], retry=False)
            except Exception:
                # If we can't even retransmit the package, we give
                # up. The consumer will timeout.
                logging.warning('Got exception retransmitting package')


class Client(object):
    def __init__(self, addrs=None, timeout=None):
        """
        @param timeout: float|None, connection timeout in seconds (per address)
        """
        if addrs is None:
            addrs = []
        self.connection = Connection(addrs, timeout)
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
