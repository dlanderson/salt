'''
SaltStreams are long-running daemon modules that have a pre-loaded
SMinion object for accessing Minion functions.

A SaltStream module should inherit the salt.streams.SaltStream class,
and should:
    - Accept two arguments to __init__:
        - The parsed minion configuration file as a dict
        - The parsed stream configuration for the module as a dict
    - Define a run() function which will be invoked by the SaltStream
        daemon spawner through multiprocessing.Process.

The SaltStream module can access the SMinion as self.minion.

For example, A SaltStream module might do something like:
    - Watch for udev events
    - Watch for upstart dbus events
    - Provide a syslog interface
    - Watch interesting files/directories

See salt.streams.SocketStream for a generic socket listener example.
'''
# Import python libs
import os
import sys
import logging
import traceback
import socket
import multiprocessing
import threading

# Import salt libs
import salt

from salt.utils.debug import enable_sigusr1_handler
from salt.utils import parsers
from salt.utils.verify import check_user, verify_env, verify_files
from salt.exceptions import (
    SaltInvocationError,
    SaltClientError,
    EauthAuthenticationError
)


class SaltStreamDaemon(parsers.SaltCallOptionParser):
    '''
    Deamon that handles loading and spwaning SaltStream IO modules
    '''
    def prepare(self):
        self.parse_args()

        if self.config['verify_env']:
            verify_env([
                    self.config['pki_dir'],
                    self.config['cachedir'],
                ],
                self.config['user'],
                permissive=self.config['permissive_pki_access'],
                pki_dir=self.config['pki_dir'],
            )
            if (not self.config['log_file'].startswith('tcp://') or
                    not self.config['log_file'].startswith('udp://') or
                    not self.config['log_file'].startswith('file://')):
                # Logfile is not using Syslog, verify
                verify_files(
                    [self.config['log_file']],
                    self.config['user']
                )

        if self.options.local:
            self.config['file_client'] = 'local'
        if self.options.master:
            self.config['master'] = self.options.master

        self.setup_logfile_logger()
        enable_sigusr1_handler()

        self.stream_configs = self.config.get('streams', {})
        self.stream_mods = salt.loader.streams(self.config)

    def start(self):
        '''
        Start the SaltStream deamon and spawn SaltStream IO modules
        '''
        self.prepare()

        active_streams = []
        for stream_id in self.stream_configs:
            stream_config = self.stream_configs[stream_id]
            stream_type = stream_config.get('type', None)
            if not stream_type:
                sys.stderr.write('Unknown stream type: {0}\n'.format(stream_type))
            if not stream_type in self.stream_mods:
                sys.stderr.write('Cannot find stream module: {0}\n'.format(stream_type))
                continue
            sys.stderr.write('Starting stream: {0} with stream_config: {1}\n'.format(stream_type, stream_config))
            stream_mod = self.stream_mods[stream_type](self.config, stream_config)
            stream_proc = multiprocessing.Process(target=stream_mod.run)
            stream_proc.start()
            active_streams.append(stream_proc)
        # TODO: clean up saltstream daemons on shutdown

class SaltStreamClient(parsers.SaltCallOptionParser):
    '''
    Client CLI process that connects to the corresponding SaltStream
    IO module
    '''
    def prepare(self):
        self.parse_args()

        if self.config['verify_env']:
            verify_env([
                    self.config['pki_dir'],
                    self.config['cachedir'],
                ],
                self.config['user'],
                permissive=self.config['permissive_pki_access'],
                pki_dir=self.config['pki_dir'],
            )
            if (not self.config['log_file'].startswith('tcp://') or
                    not self.config['log_file'].startswith('udp://') or
                    not self.config['log_file'].startswith('file://')):
                # Logfile is not using Syslog, verify
                verify_files(
                    [self.config['log_file']],
                    self.config['user']
                )

        if self.options.local:
            self.config['file_client'] = 'local'
        if self.options.master:
            self.config['master'] = self.options.master

        self.setup_logfile_logger()
        enable_sigusr1_handler()

        self.stream_configs = self.config.get('streams', {})

    def run(self):
        '''
        Connect to the SaltStream IO module stream
        '''
        self.prepare()

        # hard code for development
        self.stream_id = 'test_unix_socket'
        stream_config = self.stream_configs[self.stream_id]
        stream_type = stream_config.get('type', None)
        if not stream_type:
            sys.stderr.write('Unknown stream type: {0}\n\n'.format(stream_type))
        if not stream_type in self.stream_mods:
            sys.stderr.write('Cannot find stream module: {0}\n\n'.format(stream_type))
            sys.exit(-1)
        stream_client = self.stream_mods['{0}{1}'.format(stream_type, 'Client')](self.config, stream_config)
        stream_client.run()
