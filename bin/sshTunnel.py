#!/usr/bin/env python
#
# Copyright 2018 Odom Lab, CRUK-CI, University of Cambridge
#
# This file is part of the osqutil python package.
#
# The osqutil python package is free software: you can redistribute it
# and/or modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# The osqutil python package is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with the osqutil python package.  If not, see
# <http://www.gnu.org/licenses/>.

"""
This script starts an SSH tunnel to a given host. If the SSH process ever
dies then this script will detect that and restart it. Runtime
messages are sent to the system log.

Note that the code as written is not dependent on the osqpipe module;
if possible we should try and keep it that way.
"""

import os
from socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM
from syslog import syslog, LOG_ERR, LOG_INFO, LOG_WARNING

# Required for OneWayTunnel:
LOCAL_SSH_PORT       = 22000 # opens on localhost, connects to remote host port 22

# Also required for TwoWayTunnel:
REMOTE_SSH_PORT      = 22000 # opens on remote host, connects to localhost:22

# Required for repository database and web interface
LOCAL_HTTPS_PORT     = 443
LOCAL_SQL_PORT       = 5432
REMOTE_HTTPS_PORT    = 22443 # opens on remote host, connects to localhost:LOCAL_HTTPS_PORT
REMOTE_SQL_PORT      = 25432 # opens on remote host, connects to localhost:LOCAL_SQL_PORT

# FIXME make sure the squid proxy 3128:webcache.sanger.ac.uk:3128
# forwarding is in ~/.ssh/config prior to deploying this.

from logging import getLogger, StreamHandler, DEBUG  # For test mode only.
LOGGER = getLogger()
LOGGER.addHandler(StreamHandler())
LOGGER.setLevel(DEBUG)

##############################################################################

# There are many ways of obraining IP of the host the script is running but there
# does not seem to be one robust method working on different platforms
# Below is one of the method from stackoverflow discussion which seems to have
# robustness to an extent.
# http://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
def get_ip():
  s = socket(AF_INET, SOCK_DGRAM)
  try:
    # doesn't even have to be reachable
    s.connect(('10.255.255.255', 0))
    IP = s.getsockname()[0]
  except:
    IP = '127.0.0.1'
  finally:
    s.close()
  return IP

def kill(child, errstr='', test_mode=False):

  '''Kill a pexpect process and send details to the syslog and stdout.'''

  if not test_mode:
    syslog(LOG_ERR, errstr)
    syslog(LOG_ERR, child.before)
    syslog(LOG_ERR, child.after)
  else:
    LOGGER.error(errstr)
    LOGGER.error(child.before)
    LOGGER.error(child.after)
  child.terminate(True)
  exit(1)

def poll_reverse_tunnel():

  '''Periodically ensure that the reverse tunnel is up and
  running. Returns upon loss of connection. This is used by a copy of
  this script running on the remote host.'''

  import time

  while(1):
    sock = socket(AF_INET, SOCK_STREAM)
    res  = sock.connect_ex(('127.0.0.1', REMOTE_SSH_PORT))
    if res != 0: # Connection has failed.
      return
    time.sleep(60)

##############################################################################

class OneWayTunnel(object):

  '''Class to connect to a remote host and forward ports from target
  machines back to our local host.'''

  def __init__(self, remote_hostname, identity_file=None, test_mode=False):

    import getpass

    # Password stored in memory but slightly obfuscated; potential
    # security issue e.g. from core dumps/memory scan. Given that we
    # don't know how the pexpect code might cache the (decoded) values
    # sent to it, this seems like it might be a little pointless.
    self.gate_username = raw_input('Gateway Host Username: ').encode('base64')
    if identity_file is None:
      self.gate_password = getpass.getpass('Gateway Host Password: ').encode('base64')
    else:
      self.gate_password = getpass.getpass('Gateway Identity File Password: ').encode('base64')

    # Visible to subclasses, however they're unlikely to need access.
    self.child = None

    self.identity_file_login = False
    # N.B. we could add additional tunnels to this command, e.g. port
    # 22 for rsync. Often this will be better put in the ~/.ssh/config
    # file though.
    if identity_file is None:      
      self.ssh_flags = '-C -N -L %d:%s:22' % (LOCAL_SSH_PORT, remote_hostname)
    else:
      if os.path.isfile(identity_file):
        # NB! We omit -N flag here because it causes ssh to hang while connecting to stargate.ebi.ac.uk with identity_file specified.
        self.ssh_flags = '-i %s -C -L %d:%s:22' % (identity_file, LOCAL_SSH_PORT, remote_hostname)
      else:
        if not test_mode:
          syslog(LOG_ERR, 'Identity file not found!')
        else:
          LOGGER.error('Identity file not found!')
        exit(1)
      self.identity_file_login = True
      
    self.test_mode = test_mode

  def _start_tunnel(self, host, username, password, ssh_flags='', remote_command='', forward_tunnel=True):

    '''Given a host, username and password, connect to that host using
    the settings in ssh_flags to control the creation of an SSH
    tunnel.'''

    import pexpect
    import time

    if self.identity_file_login and forward_tunnel:
      expect_password = 'Enter passphrase for key'
      expect_password_failed = 'Enter passphrase for key'
    else:
      expect_password = 'password:'
      expect_password_failed = 'Permission denied'
      
    child = pexpect.spawn("ssh %s %s@%s %s" % (ssh_flags, username, host, remote_command))
    i = child.expect([pexpect.TIMEOUT, expect_password])
    if i == 0:
      kill(child, 'SSH timed out. Here is what SSH said:', self.test_mode)
    time.sleep(0.1)
    child.sendline(password)
    i = child.expect([pexpect.TIMEOUT, expect_password_failed])
    if i == 1:
      kill(child, 'Incorrect password. Here is what SSH said:', self.test_mode)
    return child

  def _confirm_tunnel_open(self, host):

    '''Confirms that the SSH tunnel is running and at least basically
    functional (by opening a socket connection). If there is a
    problem, kill the child SSH process with the expectation that the
    tunnel will be re-established upon the next iteration. Returns
    True if everything is okay, False if there's a problem.'''

    rc = True

    # First, detect a misbehaving child process. Also runs on
    # first loop iteration.
    if self.child is None or not self.child.isalive():

      mess = 'Restarting SSH tunnel'
      if not self.test_mode:
        syslog(LOG_WARNING, mess)
      else:
        LOGGER.warning(mess)
        
      try:
        self.child = self._start_tunnel(host,
                                        self.gate_username.decode('base64'),
                                        self.gate_password.decode('base64'),
                                        self.ssh_flags)
        mess = 'SSH tunnel established'
        if not self.test_mode:
          syslog(LOG_INFO, mess)
        else:
          LOGGER.info(mess)
          
      except Exception, err:

        if not self.test_mode:
          syslog(LOG_ERR, str(err))
        else:
          LOGGER.error(str(err))
          
        rc = False

    else:

      # Confirm that the connection is actually running.
      sock = socket(AF_INET, SOCK_STREAM)

      # This line means we always need some known port forwarding
      # encoded directly in this script; other ports can be
      # configured in ~/.ssh/config. In this case we use the squid
      # webcache port.
      res  = sock.connect_ex(('127.0.0.1', LOCAL_SSH_PORT))

      if res != 0: # Connection has mysteriously failed.

        mess = 'Shutting down link upon unexplained connection failure'
        if not self.test_mode:
          syslog(LOG_WARNING, mess)
        else:
          LOGGER.warning(mess)

        # FIXME could also consider child.terminate below, if
        # child.close generates zombie processes.
        self.child.close(True) # Will attempt to restart on the next cycle.
        rc = False

    return rc

  def connect(self, host):

    '''Start a background daemon which sets up the SSH tunnel and
    monitors it, restarting whenever necessary.'''

    import daemon
    import time
    
    if not self.test_mode:

      print "Starting daemon for SSH tunnel to %s..." % host
      with daemon.DaemonContext():

        # From here on in, we have no access to stdout/stderr to inform
        # the user of problems.
        while True:
          rc = self._confirm_tunnel_open(host)
          if rc:  # Don't hang around if there's a problem.
            time.sleep(60)

    else: # Test mode; don't detach from console.
      print "Running SSH tunnel under test mode to %s..." % host
      while True:
        rc = self._confirm_tunnel_open(host)
        if rc:  # Don't hang around if there's a problem.
          time.sleep(60)
        
##############################################################################

class TwoWayTunnel(OneWayTunnel):

  '''Class which creates a local->target tunnel in addition to the
  target->local tunnel created by its superclass.'''

  def __init__(self, remote_dir='.', localhost_ip=None, rep_tunnels=False, *args, **kwargs):

    import getpass
    
    super(TwoWayTunnel, self).__init__(*args, **kwargs)

    print "\nLeave Remote details blank if the same as the Gateway login account:"
    username = raw_input('  Remote Host Username: ')
    password = getpass.getpass('  Remote Host Password: ')

    if (len(username) == 0):
      self.remote_username = self.gate_username
    else:
      self.remote_username = username.encode('base64')

    if (len(password) == 0):
      self.remote_password = self.gate_password
    else:
      self.remote_password = password.encode('base64')

    self.grandchild = None
    self.remote_dir = remote_dir

    if localhost_ip is None:
      localhost_ip = get_ip()
      mess = 'Local IP: %s' % localhost_ip
      if not self.test_mode:
        syslog(LOG_INFO, mess)
      else:
        LOGGER.info(mess)
    
    # Need to grab this before the daemon detaches from the tty.
    self.local_dir  = os.getcwd()

    # Add reverse tunnel flags for repository support: port 443 (https) and port 5432 (PostgreSQL)
    rep_tunnel_str = ''
    if rep_tunnels:
      rep_tunnel_str = ' -R %s:%s:%s -R %s:%s:%s' % (REMOTE_HTTPS_PORT, localhost_ip, LOCAL_HTTPS_PORT, REMOTE_SQL_PORT, localhost_ip, LOCAL_SQL_PORT)

    # For forwarding other ports, e.g. https port 443, look into the
    # ~/.ssh/config file using host-specific RemoteForward
    # directives. Note that we omit the -N option here because we want
    # to run a script on the remote host to check for port integrity.
    self.reverse_ssh_flags = '-p %d -C -R %d:%s:22%s' % (LOCAL_SSH_PORT, REMOTE_SSH_PORT, localhost_ip, rep_tunnel_str)

  def _scp_script_remote(self):

    '''Copies this script to the target path on the server.'''

    import pexpect

    scriptfile = os.path.join(self.local_dir, __file__)
    cmd = ('scp -P %d %s %s@127.0.0.1:%s/.'
           % (LOCAL_SSH_PORT, scriptfile,
              self.remote_username.decode('base64'), self.remote_dir))

    mess = 'Copying SSH tunnel script to remote server (%s).' % cmd
    if not self.test_mode:
      syslog(LOG_WARNING, mess)
    else:
      mess += ": %s" % cmd
      LOGGER.info(mess)
      
    child = pexpect.spawn(cmd)
    i = child.expect(['assword:', r"yes/no"], timeout=30)
    if i == 0:
      child.sendline(self.remote_password.decode('base64'))
    elif i == 1:
      child.sendline("yes")
      child.expect("assword:", timeout=30)
      child.sendline(self.remote_password.decode('base64'))
    data = child.read()
    child.close()

  def _confirm_tunnel_open(self, *args, **kwargs):

    '''Confirms that the forward tunnel is up and running; then
    confirms that the reverse tunnel is also running.'''

    # First make sure the forward tunnel is up.
    rc = super(TwoWayTunnel, self)._confirm_tunnel_open(*args, **kwargs)

    # Confirm that the reverse tunnel is running.
    if rc:

      # First run through, copy this script to the remote server.
      if self.grandchild is None:
        self._scp_script_remote()

      # Set up the reverse tunnel here. We need an SSH session going
      # out to the remote host over the previously-established tunnel to
      # the gateway host. We then run a copy of this script on the remote host
      # with the --revpoll option to check that the forwarded port
      # link is maintained.
      if self.grandchild is None or not self.grandchild.isalive():

        mess = 'Restarting SSH reverse tunnel'
        if not self.test_mode:
          syslog(LOG_WARNING, mess)
        else:
          LOGGER.warning(mess)
          
        try:
          self.grandchild = self._start_tunnel('127.0.0.1',
                                               self.remote_username.decode('base64'),
                                               self.remote_password.decode('base64'),
                                               self.reverse_ssh_flags,
                                               os.path.join(self.remote_dir, os.path.basename(__file__)) + ' --revpoll', forward_tunnel=False)
          mess = 'SSH reverse tunnel established'
          if not self.test_mode:
            syslog(LOG_INFO, mess)
          else:
            LOGGER.info(mess)
            
        except Exception, err:

          if not self.test_mode:
            syslog(LOG_ERR, str(err))
          else:
            LOGGER.error(str(err))
            
          rc = False
      
    return rc

##############################################################################

if __name__ == '__main__':

  # from argparse import ArgumentParser
  import argparse
  import textwrap

  PARSER = argparse.ArgumentParser(description = 'Set up and maintain an SSH tunnel to a remote host.',
                          formatter_class=argparse.RawDescriptionHelpFormatter,
                          epilog = textwrap.dedent('''\
                          Examples:
                            sshTunnel.py -t --gateway-host ssh.sanger.ac.uk --remote-host seq3b.internal.sanger.ac.uk
                            sshTunnel.py -t --gateway-host stargate.ebi.ac.uk --identity-file /home/user/.ssh/ebi_key --remote-host login-r6-2.ebi.ac.uk
                            sshTunnel.py -t --gateway-host stargate.ebi.ac.uk --identity-file /home/user/.ssh/ebi_key --remote-host hh-yoda-11-01.ebi.ac.uk --repository-tunnels
                          '''))

  PARSER.add_argument('-t', '--twoway', dest='twoway', action='store_true',
                      help='Set up the tunnel in both directions. The default action'
                         + ' is to set up in only a single direction.')

  PARSER.add_argument('-p', '--revpoll', dest='revpoll', action='store_true',
                      help='Poll the reverse tunnel to monitor the connection.'
                         + ' This run mode is only used on the remote host and should'
                         + ' not be invoked manually. Exits upon connection failure.')

  PARSER.add_argument('-d', '--remotedir', dest='remdir', type=str, default='.',
                      help='The directory on the remote machine into which this script'
                         + ' will be copied. Two-way tunnels only.')

  PARSER.add_argument('--gateway-host', dest='gateway', type=str, default='ssh.sanger.ac.uk',
                      help='The gateway SSH host (default: ssh.sanger.ac.uk).')

  PARSER.add_argument('--identity-file', dest='identity', type=str, default=None,
                      help='A file from which the identity (private key) for public key authentication is read. (default: None)')

  PARSER.add_argument('--local_ssh_port', dest='local_ssh_port', type=int, default=None,
                      help='ssh port in local host for entering the tunnel (default: %d).' % LOCAL_SSH_PORT)

  PARSER.add_argument('--remote_ssh_port', dest='remote_ssh_port', type=int, default=None,
                      help='ssh port in remote server for reverse tunnel entry back to local host (default: %d).' % REMOTE_SSH_PORT)
  
  PARSER.add_argument('--remote-host', dest='remote', type=str,
                      default='seq3b.internal.sanger.ac.uk',
                      help='The remote internal SSH host'
                      + ' (default: seq3b.internal.sanger.ac.uk).')
  PARSER.add_argument('--local-hostip', dest='localhost_ip', type=str,
                      default=None,
                      help='IP of the local host (default: autodetect local IP)')

  PARSER.add_argument('--testmode', dest='testmode', action='store_true',
                      help='Run the script in test mode. This will prevent the script'
                         + ' from detaching from the console as a daemon, and produce'
                         + ' debugging messages.')
  PARSER.add_argument('--repository-tunnels', dest='rep_tunnels', action='store_true',
                      help='Adds reverse tunnels to https (port 443) and PostgreSQL (port 5432) servers in local host.')

  ARGS = PARSER.parse_args()

  if ARGS.local_ssh_port:  
    LOCAL_SSH_PORT = ARGS.local_ssh_port

  if ARGS.remote_ssh_port:
    REMOTE_SSH_PORT = ARGS.remote_ssh_port
    
  if ARGS.revpoll:
    # Speciality run mode called by the TwoWayTunnel class to monitor
    # the reverse tunnel.
    poll_reverse_tunnel()
    exit(1)

  # Here's where the tunnels actually get set up.
  if ARGS.twoway:
    print "Setting up a Two-way tunnel to the remote host."
    TUNNEL = TwoWayTunnel(remote_hostname=ARGS.remote, identity_file=ARGS.identity,
                          test_mode=ARGS.testmode,
                          remote_dir=ARGS.remdir, localhost_ip=ARGS.localhost_ip, rep_tunnels=ARGS.rep_tunnels)
  else:
    print "Setting up a One-way tunnel to the remote host."
    TUNNEL = OneWayTunnel(remote_hostname=ARGS.remote, identity_file=ARGS.identity,
                          test_mode=ARGS.testmode)
  TUNNEL.connect(ARGS.gateway)
