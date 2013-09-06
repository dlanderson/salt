# Import python libs
import os
import sys
import logging
import re
import socket
import SocketServer
import threading
import time

# Import salt libs
import salt
from salt._compat import string_types
from salt.streams import SaltStream

DEFAULT_BUFFER_SIZE = 4096
DEFAULT_BACKLOG = 5

class SingleSocketStreamHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        stream_config = self.server.stream_mod.stream_config
        if 'fun' in stream_config:
            fun_name = stream_config['fun'].get('name', 'test.ping')
            fun_arg = stream_config['fun'].get('arg', [])
        while True:
            data = self.request.recv(
                                self.server.socket_config['opts'].get(
                                    'buffer_size', DEFAULT_BUFFER_SIZE
                                    )
                                )
            if not data:
                break
            ret = self.server.stream_mod.call(fun_name, fun_arg)
            self.request.send(self.server.stream_mod.serial.dumps(ret))

class UnixSocketStreamServer(SocketServer.ThreadingUnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, stream_mod, socket_config):
        self.stream_mod = stream_mod
        self.socket_config = socket_config
        SocketServer.ThreadingUnixStreamServer.__init__(
                                        self,
                                        self.socket_config['address'],
                                        SingleSocketStreamHandler
                                    )

class TCPSocketStreamServer(SocketServer.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, stream_mod, socket_config):
        self.stream_mod = stream_mod
        self.socket_config = socket_config
        SocketServer.ThreadingTCPServer.__init__(
                                    self,
                                    (self.socket_config['address'],
                                    int(self.socket_config['port'])),
                                    SingleSocketStreamHandler
                                )

class UDPSocketStreamServer(SocketServer.ThreadingUDPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, stream_mod, socket_config):
        self.stream_mod = stream_mod
        self.socket_config = socket_config
        SocketServer.ThreadingUDPServer.__init__(
                                    self,
                                    (self.socket_config['address'],
                                    int(self.socket_config['port'])),
                                    SingleSocketStreamHandler
                                )

class SocketStream(salt.streams.SaltStream):
    '''
    Object to wrap the calling of local salt modules for the salt-call command
    '''
    def __init__(self, opts, stream_config):
        self.opts = opts
        self.stream_config = stream_config
        self.socket_defaults = self._get_socket_defaults()
        self.listen_sockets = self._parse_socket_configs()
        super(SocketStream, self).__init__(self.opts)

    def _get_socket_defaults(self):
        '''
        Check for 'defaults' hash in the 'sockets' section of
        the stream_config
        '''
        socket_defaults = {}
        socket_configs = self.stream_config.get('sockets', {})
        for config in socket_configs:
            if isinstance(config, dict):
                if 'defaults' in config:
                    socket_defaults = config['defaults']
        return socket_defaults

    def _parse_socket_configs(self):
        '''
        Parse the 'sockets' list in the stream_config and return a list
        of hashes with valid addresses and any options set per socket.

        Each socket hash will contain these fields:
            { address: '', type: '', port: '', opts: {} }
        '''
        valid_sockets = []
        address_matcher = re.compile(r"^(?P<type>tcp|udp|unix)://"
                            r"(\[)?(?P<address>.*?)(\])?"
                            r"(:(?P<port>\d+))?$"
                        )
        for unparsed_sock in self.stream_config.get('sockets', {}):
            if isinstance(unparsed_sock, string_types):
                try:
                     sock_config = address_matcher.match(
                                                unparsed_sock
                                                ).groupdict()
                except AttributeError:
                    # no match, log something
                    continue
                sock_config['opts'] = {}
                valid_sockets.append(sock_config)
            elif isinstance(unparsed_sock, dict):
                unparsed_addr = list(unparsed_sock)[0]
                try:
                    sock_config = address_matcher.match(
                                                unparsed_addr
                                                ).groupdict()
                except AttributeError:
                    # no match, log something
                    continue
                sock_config['opts'] = unparsed_sock[unparsed_addr]
                valid_sockets.append(sock_config)
        return valid_sockets

    def _setup_socket_server(self, socket_config):
        '''
        Configure a threaded SocketServer based on socket_config.
        Return the SocketServer object.
        '''
        if socket_config['type'] == 'unix':
            try:
                os.unlink(socket_config['address'])
            except OSError:
                if os.path.exists(socket_config['address']):
                    raise
            socket_server = UnixSocketStreamServer(self, socket_config)
        elif socket_config['type'] == 'tcp':
            socket_server = TCPSocketStreamServer(self, socket_config)
        elif socket_config['type'] == 'udp':
            socket_server = UDPSocketStreamServer(self, socket_config)
        return socket_server

    def run(self):
        '''
        Main SocketStream process function that spawns
        threaded socket servers
        '''
        for socket_config in self.listen_sockets:
            socket_server = self._setup_socket_server(socket_config)
            server_thread = threading.Thread(
                                    target=socket_server.serve_forever
                                    )
            server_thread.start()

        while True:
            time.sleep(1)
        # TODO: clean up threads properly on shutdown

class SocketStreamClient(object):
    def __init__(self, opts, stream_config):
        self.opts = opts
        self.stream_config = stream_config
        self.sock_address = stream_config.get('sock_address', '')
        self.sock_type = 'UNIX'

    def run(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.sock_address)
        while True:
            try:
                data = raw_input()
            except EOFError:
                sys.exit(0)
            sock.sendall(data)
            ret = sock.recv(1024)
            print ret
