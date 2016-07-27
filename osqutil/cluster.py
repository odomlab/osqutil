'''
Module defining some base classes used in constructing and
submitting cluster jobs. Includes AlignmentManager classes which are
run directly on cluster nodes (and can therefore have no non-core
external module dependencies).
'''

import sys
import os
import re
import logging
import glob
import tempfile
import uuid

from subprocess import Popen, PIPE
import time

from pipes import quote
from tempfile import gettempdir
from shutil import move

from osqutil.utilities import call_subprocess, bash_quote, \
    is_zipped, is_bzipped, set_file_permissions, BamPostProcessor, \
    parse_repository_filename, write_to_remote_file

from osqutil.config import Config

from osqutil.setup_logs import configure_logging
LOGGER = configure_logging('osqutil.cluster')

##############################################################################

def make_bam_name_without_extension(fqname):

  """Creates bam file basename out of Odom/Carroll lab standard fq
  filename. Note that this function does not append '.bam' to the
  returned name, so that samtools can add it for us."""

  # before further processing, make sure the input filename is stipped back from compression prefix.
  if fqname.endswith('.gz') or fqname.endswith('.bz2'):
    fqname = os.path.splitext(fqname)[0]
  base         = os.path.splitext(fqname)[0]
  lane_pattern = re.compile(r'^(.*)p[12](@\d+)?$')
  matchobj     = lane_pattern.match(base)
  if matchobj != None:
    base = matchobj.group(1)
    if matchobj.group(2):
      base += matchobj.group(2)
  return base


##############################################################################
##############################################################################

class SimpleCommand(object):
  '''
  Simple class used as a default command-string builder.
  '''
  __slots__ = ('conf')

  def __init__(self):

    self.conf = Config()

  def build(self, cmd, *args, **kwargs):
    
    if type(cmd) in (str, unicode):
      cmd = [cmd]

    return " ".join(cmd)

class NohupCommand(SimpleCommand):
  '''
  Class used to wrap a command in a nohup nice invocation for
  execution on a remote desktop.
  '''
  def build(self, cmd, remote_wdir, *args, **kwargs):

    cmd = super(NohupCommand, self).build(cmd, *args, **kwargs)

    # Now we set off a process on the target server which will align
    # the file(s) and scp the result back to us. Note the use of
    # 'nohup' here. Also 'nice', since we're probably using someone's
    # desktop computer. Command is split into two parts for clarity:
    cmd = ("nohup nice -n 20 sh -c '( (%s" % cmd)

    # Closing out the nohup subshell. See this thread for discussion:
    # http://stackoverflow.com/a/29172
    cmd += (") &)' >> %s 2>&1 < /dev/null"
            % os.path.join(remote_wdir, 'remote_worker.log'))

    return cmd

class SbatchCommand(SimpleCommand):
  '''
  Class used to build sbatch-wrapped command.
  '''
  def build(self, cmd, mem=2000, queue=None, jobname=None,
            auto_requeue=False, depend_jobs=None, sleep=0, 
            mincpus=1, maxcpus=1, clusterlogdir=None, environ=None, *args, **kwargs):
    # The environ argument allows the caller to pass in arbitrary
    # environmental variables (e.g., JAVA_HOME) as a dict.
    if environ is None:
      environ = {}

    # Pass the PYTHONPATH to the cluster process. This allows us to
    # isolate e.g. a testing instance of the code from production.
    # Note that we can't do this as easily for PATH itself because
    # bsub itself is in a custom location on the cluster.
    for varname in ('PYTHONPATH', 'OSQPIPE_CONFDIR'):
      if varname in os.environ:
        environ[varname] = os.environ[varname]

    cmd = super(BsubCommand, self).build(cmd, *args, **kwargs)

    # Add information about environment in front of the command.
    envstr = " ".join([ "%s=%s" % (key, val) for key, val in environ.iteritems() ])
    cmd = envstr + " " + cmd
    
    # In some cases it is beneficial to wait couple of seconds before the job is executed
    # As the job may be executed immediately, we add little wait before the execution of the rest of the command.
    if sleep > 0:
      cmd = ('sleep %d && ' % sleep) + cmd
           
    # We will create a slurm batch script locally and set the local and foreign path for the slurm file.
    # NB! It may be more sensible to write all the script commands into a string and then write this string into a foreign file in cluster
    #     using write_to_remote_file(ncommands, nfname, self.conf.clusteruser, self.conf.cluster)
    # Note that on execution in cluster, the script will rename itself from slurmfile to <jobid>.sh.
    # uuid.uuid1() creates unique string based on hostname and time.
    ltmpdir = tempfile.gettempdir()
    slurmfile = str(uuid.uuid1())
    lslurmfile = os.path.join(ltmpdir, slurmfile)
    fslurmfile = os.path.join(clusterlogdir, slurmfile)    

    f = open(lslurmfile, 'wb')
    
    # Create sbatch bash script  
    f.write('#!/bin/bash\n')
    if jobname is not None:
      f.write('#SBATCH -J %s\n' % jobname) # where darwinjob is the jobname
    # f.write('#SBATCH -A CHANGEME\n') # which project should be charged. I think at this point this can be commented out.
    # A safety net in case min or max nr of cores gets muddled up. An
    # explicit error is preferred in such cases, so that we can see
    # what to fix.
    if mincpus > maxcpus:
      maxcpus = mincpus
      LOGGER.info("mincpus (%d) is greater than maxcpus (%d). Maxcpus was made equal to mincpus!" % (mincpus, maxcpus))
    f.write('#SBATCH --nodes=%d-%d\n' % (mincpus, maxcpus) ) # how many whole nodes (cores) should be allocated
    f.write('#SBATCH -N 1\n') # Make sure that all cores are in one node
    f.write('#SBATCH --mail-type=NONE\n') # never receive mail
    f.write('#SBATCH -p %s\n' % queue) # Queue where the job is sent.
    f.write('#SBATCH --open-mode=append\n') # record information about job re-sceduling
    if auto_requeue:
      f.write('#SBATCH --requeue\n') # requeue job in case node dies etc.
    else:
      f.write('#SBATCH --no-requeue\n') # do not requeue the job    
    f.write('#SBATCH --mem %s\n' % mem) # memory in MB
    # f.write('#SBATCH -t 0-%s\n' % time_limit) # Note that time_limit is a string in format of hh:mm
    f.write('#SBATCH -o %s/%%j.stdout\n' % clusterlogdir) # File to which STDOUT will be written
    f.write('#SBATCH -e %s/%%j.stderr\n' % clusterlogdir) # File to which STDERR will be written
    if depend_jobs is not None:
      dependencies = '#SBATCH --dependency=aftercorr' # execute job after all corresponding jobs 
      for djob in depend_jobs:
        dependencies += ':%s' % djob
      f.write('%s\n' % dependencies)
    # Following (two) lines are not necessarily needed but suggested by University Darwin cluster for record keeping in scheduler log files.
    f.write('numnodes=$SLURM_JOB_NUM_NODES\n')
    f.write('numtasks=$SLURM_NTASKS\n')
    f.write('hostname=`hostname`\n')
    f.write('workdir=\"$SLURM_SUBMIT_DIR\"\n')
    # This is the place where the actual command we want to execute is added to the script.
    f.write('CMD=\"%s\"\n' % cmd)
    # Change dir to work directory.
    f.write('cd %s\n' % self.conf.clusterworkdir)    
    f.write('echo -e \"Changed directory to `pwd`.\n\"\n')
    f.write('JOBID=$SLURM_JOB_ID\n')
    f.write('echo -e \"JobID: $JOBID\n======\"\n')
    f.write('echo "Job start time: `date`"\n')
    f.write('echo \"Executed in node: $hostname\"\n')
    f.write('echo \"CPU info: `cat /proc/cpuinfo | grep name | uniq | tr -s \' \' | cut -f2 -d:`\"\n')
    f.write('echo \"Current directory: `pwd`\"\n')
    # f.write('echo -e \"\nnumtasks=$numtasks, numnodes=$numnodes\"\n')     
    f.write('echo -e \"Number of cores requested: min=%d, max=%d\"\n' % (mincpus, maxcpus))    
    f.write('echo -e \"Number of nodes received: $numnodes\"\n')
    f.write('echo -e \"\nExecuting command:\n==================\n$CMD\n\"\n')    
    f.write('mv %s %s/$SLURM_JOB_ID.sh\n' % (fslurmfile, clusterlogdir))
    f.write('eval $CMD\n\n')
    f.write('echo "Job end time: `date`"\n')
    f.close()

    # Copy slurm file to clusterlogdir in cluster head node.
    rjr = RemoteJobRunner(transfer_wdir=clusterlogdir)
    rjr.remote_copy_files(filenames=[lslurmfile],destnames=[slurmfile])
    
    # Remove slurm file locally.
    os.remove(lslurmfile)
    
    # Create slurm command
    slurmcmd = 'sbatch %s' % fslurmfile

    return slurmcmd
  
class BsubCommand(SimpleCommand):
  '''
  Class used to build a bsub-wrapped command.
  '''
  def build(self, cmd, mem=2000, queue=None, jobname=None,
            auto_requeue=False, depend_jobs=None, sleep=0, 
            mincpus=1, maxcpus=1, clusterlogdir=None, environ=None, *args, **kwargs):

    # The environ argument allows the caller to pass in arbitrary
    # environmental variables (e.g., JAVA_HOME) as a dict.
    if environ is None:
      environ = {}

    # Pass the PYTHONPATH to the cluster process. This allows us to
    # isolate e.g. a testing instance of the code from production.
    # Note that we can't do this as easily for PATH itself because
    # bsub itself is in a custom location on the cluster.
    for varname in ('PYTHONPATH', 'OSQPIPE_CONFDIR'):
      if varname in os.environ:
        environ[varname] = os.environ[varname]

    cmd = super(BsubCommand, self).build(cmd, *args, **kwargs)

    # Note that if this gets stuck in an infinite loop you will need
    # to use "bkill -r" to kill the job on LSF. N.B. exit code 139 is
    # a core dump. But so are several other exit codes; add 128 to all
    # the unix signals which result in a dump ("man 7 signal") for a
    # full listing.
    qval = "-Q 'all ~0'" if auto_requeue else ''
    try:
      group = self.conf.clustergroup
      if group != '':
        group = '-G ' + group
    except AttributeError:
      group = ''

    resources = 'rusage[mem=%d]' % mem
    memreq    = ''

    # Sanger cluster (farm3) has a stricter set of requirements which
    # are not supported by our local cluster. Quelle surprise.
    try:
      provider = self.conf.clusterprovider
      if provider[:3].lower() == 'san':
        resources = ('select[mem>%d] ' % mem) + resources
        memreq    = '-M %d' % mem
    except AttributeError:
      pass

    # A safety net in case min or max nr of cores gets muddled up. An
    # explicit error is preferred in such cases, so that we can see
    # what to fix.
    if mincpus > maxcpus:
      maxcpus = mincpus
      LOGGER.info("mincpus (%d) is greater than maxcpus (%d). Maxcpus was made equal to mincpus!" % (mincpus, maxcpus))

    # In case clusterlogdir has been specified, override the self.conf.clusterstdout
    # This is handy in case we want to keep the logs together with job / larger project related files.
    cluster_stdout_stderr = ""
    if clusterlogdir is not None:
      cluster_stdout_stderr = "-o %s/%%J.stdout -e %s/%%J.stderr" % (clusterlogdir, clusterlogdir)
    else:
      cluster_stdout_stderr = "-o %s/%%J.stdout -e %s/%%J.stderr" % (self.conf.clusterstdoutdir, self.conf.clusterstdoutdir)

    envstr  = " ".join([ "%s=%s" % (key, val) for key, val in environ.iteritems() ])
    bsubcmd = (("%s bsub -R '%s' -R 'span[hosts=1]'"
           + " %s -r %s -n %d,%d"
                + " %s %s")
           % (envstr,
              resources,
              memreq,
              cluster_stdout_stderr,
              mincpus,
              maxcpus,
              qval,
              group))

    if queue is not None:
      bsubcmd += ' -q %s' % queue

    # The jobname attribute is also used to control LSF job array creation.
    if jobname is not None:
      bsubcmd += ' -J %s' % jobname

    if depend_jobs is not None:
      depend = "&&".join([ "ended(%d)" % (x,) for x in depend_jobs ])
      bsubcmd += " -w '%s'" % depend

    if sleep > 0:
      cmd = ('sleep %d && ' % sleep) + cmd

    # To group things in a pipe (allowing e.g. use of '&&'), we use a
    # subshell. Note that we quote the sh -c string once, and
    # everything within that string twice. Commands can be of the following form:
    #
    # "ssh blah@blah.org 'echo stuff | target.txt'"
    # r"ssh blah@blah.org \"echo stuff | target.txt\""
    #
    # I.e., one needs to be careful of python's rather idiosyncratic
    # string quoting rules, and use the r"" form where necessary.
    bsubcmd += r' sh -c "(%s)"' % re.sub(r'"', r'\"', cmd)

    return bsubcmd

##############################################################################
class JobRunner(object):
  '''
  Instantiable base class used as a core definition of how the various
  job submitter classes are organised. Each JobRunner subclass has a
  command_builder attribute which is used to create the final command
  string that is executed. The default behaviour of this class is to
  simply run the command using call_subprocess on the current
  host. Various subclasses have been created for extending this to
  execute the command on a remote host, or on an LSF cluster. Note
  that to submit to a local LSF head node, one might use this::

  jr = JobRunner(command_builder=BsubCommand())
  jr.submit_command(cmd, mem=8000, queue='dolab')

  See the ClusterJobSubmitter class for how this has been extended to
  submitting to a remote LSF head node.
  '''
  __slots__ = ('test_mode', 'config', 'command_builder')
  def __init__(self, test_mode=False, command_builder=None, *args, **kwargs):
    self.test_mode = test_mode
    if test_mode:
      LOGGER.setLevel(logging.DEBUG)
    else:
      LOGGER.setLevel(logging.INFO)
      
    self.config = Config()
    
    self.command_builder = SimpleCommand() \
        if command_builder is None else command_builder

  def run_command(self, cmd, tmpdir=None, path=None, command_builder=None, *args, **kwargs):

    if command_builder:
      cmd = command_builder.build(cmd, *args, **kwargs)
    else:
      cmd = self.command_builder.build(cmd, *args, **kwargs)
      
    if path is None:
      path = self.config.hostpath
      
    if tmpdir is None:
      tmpdir = gettempdir()
      
    LOGGER.debug(cmd)
    if not self.test_mode:
      return call_subprocess(cmd, shell=True, path=path, tmpdir=tmpdir)
    return None

  def submit_command(self, *args, **kwargs):
    '''
    Submit a remote command to whatever queuing or backgrounding
    mechanism the host server supports. Typically this method should
    be used for the big jobs, and run_command for trivial,
    order-sensitive things like checking for the existence of a genome
    on the server, or uncompressing remote files.
    '''
    return self.run_command(*args, **kwargs)

class JobSubmitter(JobRunner):

  '''Class to run jobs via LSF/bsub on the local host (i.e., when running on the cluster).'''
  
  def __init__(self, remote_wdir=None, *args, **kwargs):
    self.conf = Config()
    super(JobSubmitter, self).__init__(command_builder=BsubCommand(),
                                       *args, **kwargs)

  def submit_command(self, cmd, *args, **kwargs):
    '''
    Submit a job to run on the cluster. Uses bsub to enter jobs into
    the LSF queuing system. Extra arguments are passed to
    BsubCommand.build(). The return value is the integer LSF job ID.
    '''
    pout = super(JobSubmitter, self).\
           submit_command(cmd,
                          *args, **kwargs)
    
    # FIXME this could be farmed out to utilities?
    jobid_pattern = re.compile(r"Job\s+<(\d+)>\s+is\s+submitted\s+to")
    for line in pout:
      matchobj = jobid_pattern.search(line)
      if matchobj:
        jobid = int(matchobj.group(1))
        LOGGER.info("LSF ID of submitted job: %d", jobid)
        return jobid
        
    raise ValueError("Unable to parse bsub output for job ID.")

class RemoteJobRunner(JobRunner):
  '''
  Abstract base class holding some common methods used by classes
  controlling alignment job submission to the cluster and to other
  computing resources.
  '''
  remote_host = None
  remote_port = 22  # ssh default
  remote_user = None
  remote_wdir = None
  transfer_host = None
  transfer_wdir = None

  def __init__(self, *args, **kwargs):

    # A little programming-by-contract, as it were.
    if not all( x in self.__dict__.keys()
                for x in ('remote_host', 'remote_user', 'remote_wdir',
                          'transfer_host', 'transfer_wdir')):
      raise StandardError("Remote host information not provided.")
    super(RemoteJobRunner, self).__init__(*args, **kwargs)

  def run_command(self, cmd, wdir=None, path=None, command_builder=None, *args, **kwargs):
    '''
    Method used to run a command *directly* on the remote host. No
    attempt will be made to use any kind of queuing or backgrounding
    mechanism.

    The command call is wrapped in an ssh connection. This command will
    also automatically change to the configured remote working
    directory before executing the command.
    '''
    if command_builder:
      cmd = command_builder.build(cmd, *args, **kwargs)
    else:
      cmd = self.command_builder.build(cmd, *args, **kwargs)
      
    if wdir is None:
      wdir = self.remote_wdir

    if path is None:
      pathdef = ''
    else:
      if type(path) is list:
        path = ":".join(path)
      pathdef = "PATH=%s" % path

    cmd = ("ssh -p %s %s@%s \"source /etc/profile; cd %s && %s %s\""
           % (str(self.remote_port),
              self.remote_user,
              self.remote_host,
              wdir,
              pathdef,
              re.sub(r'"', r'\"', cmd)))
    LOGGER.debug(cmd)
    if not self.test_mode:
      return call_subprocess(cmd, shell=True, path=self.config.hostpath)
    return None

  def find_remote_executable(self, progname, path=None):
    '''
    Quick (and a little dirty) approach to identifying an executable
    file on the specified path on a remote server (defaults to the
    default shell $PATH var).
    '''
    # This is a fairly simple shell script which just iterates over
    # the elements in $PATH and returns the first hit for an
    # executable file of the specified progname.
    cmd = (r"""IFS=':' read -a myary <<< \$PATH && for elem in \${myary[@]};"""
           + (r""" do if [ -x \${elem}/%s ]; then found=\${elem}/%s; break; fi; done && echo \$found"""
              % (progname, progname)))

    # Run the command directly on the server (without bsub or nohup).
    output = self.run_command(cmd, path=path, command_builder=SimpleCommand())

    # One or zero lines should be returned.
    line = output.next()
    if line is None:
      return None

    executable = line.strip()
    if executable == '':
      return None

    LOGGER.debug('Found remote executable at %s', executable)
    return executable

  def remote_copy_files(self, filenames, destnames=None, same_permissions=False):
    '''
    Copy a set of files across to the remote working directory.
    '''
    if destnames is None:
      destnames = filenames
    if len(filenames) != len(destnames):
      raise ValueError("If used, the length of the destnames list"
                                                                          + " must equal that of the filenames list.")
    for i in range(0, len(filenames)):
      fromfn = filenames[i]
      destfn = destnames[i]
      
      destfile = os.path.join(self.transfer_wdir, destfn)
      destfile = bash_quote(destfile)
      
      # Currently we assume that the same login credentials work for
      # both the cluster and the data transfer host. Note that this
      # needs an appropriate ssh key to be authorised on both the
      # transfer host and the cluster host.
      cmdbits = ['scp', '-P', str(self.remote_port)]
      if same_permissions: # default is to use the configured umask.
        cmdbits += ['-p']
      cmdbits += ['-q', bash_quote(fromfn),
                  "%s@%s:%s" % (self.remote_user,
                                self.transfer_host,
                                quote(destfile))]
      cmd = " ".join(cmdbits)

      LOGGER.debug(cmd)
      if not self.test_mode:
        call_subprocess(cmd, shell=True, path=self.config.hostpath)

  def remote_uncompress_file(self, fname, zipcommand='gzip'):
    '''
    Given a remote filename, run the specified zip command via ssh
    pipe to uncompress it. Return the filename with .gz/.bz2 suffix
    removed. Note that the zip command must understand -f and -d
    options as typically passed to gzip, and must strip off the
    filename extension for the uncompressed output file as one would
    expect.
    '''
    # Note that we're assuming that the name extensions reflect the
    # compression status.
    destfile = os.path.join(self.remote_wdir, fname)
  
##  Note that double-quoting here gives undesired results if the
##  filename contains square brackets. If problems recur with other
##  filenames, consider modifying bash_quote to omit the
##  square-bracket quoting.
#    destfile = bash_quote(destfile)
    
    # Assumes that gzip/bzip2/whatever is in the executable path on
    # the remote server.
    LOGGER.info("Uncompressing remote file %s", fname)
    cmd = " ".join(('%s -f -d' % zipcommand, quote(destfile)))
    self.run_command(cmd, command_builder=SimpleCommand())
    
    # Remove the filename extension.
    return os.path.splitext(fname)[0]

  def transfer_data(self, filenames, destnames=None):
    '''
    Convenience method to copy data files across to the server,
    uncompress if necessary, and return the fixed destination
    filenames.
    '''
    if destnames is None:
      destnames = filenames

    # Copy the files across.
    self.remote_copy_files(filenames, destnames)

    # Next, call uncompress any files which need it.
    uncomp_names = []
    for num in range(len(destnames)):
      
      # We have to test the file we copied over, since we'll be
      # reading its header.
      if is_zipped(filenames[num]):
        uncomp = self.remote_uncompress_file(destnames[num], zipcommand='gzip')
      elif is_bzipped(filenames[num]):
        uncomp = self.remote_uncompress_file(destnames[num], zipcommand='bzip2')
      else:
        uncomp = os.path.join(self.remote_wdir, destnames[num])
      uncomp_names.append(uncomp)

    return uncomp_names
  
  def submit(self, *args, **kwargs):
    '''
    Stub method identifying this as an abstract base class. This is
    typically the primary method which defines the command to be run,
    and which calls self.transfer_data() and
    self.submit_command().
    '''
    raise NotImplementedError(
      "Attempted to submit remote job via an abstract base class.")

##############################################################################
##############################################################################
class ClusterJobSubmitter(RemoteJobRunner):

  '''Class to run jobs via LSF/bsub on the cluster.'''

  def __init__(self, remote_wdir=None, *args, **kwargs):

    self.conf        = Config()
    self.remote_host = self.conf.cluster
    self.remote_port = self.conf.clusterport
    self.remote_user = self.conf.clusteruser
    self.remote_wdir = self.conf.clusterworkdir if remote_wdir is None else remote_wdir
    try:
      self.transfer_host = self.conf.transferhost
    except AttributeError, _err:
      LOGGER.debug("Falling back to cluster host for transfer.")
      self.transfer_host = self.remote_host
    try:
      self.transfer_wdir = self.conf.transferdir
    except AttributeError, _err:
      LOGGER.debug("Falling back to cluster remote directory for transfer.")
      self.transfer_wdir = self.remote_wdir

    # Must call this *after* setting the remote host info.
    super(ClusterJobSubmitter, self).__init__(command_builder=BsubCommand(),
                      *args, **kwargs)

  def submit_command(self, cmd, *args, **kwargs):
    '''
    Submit a job to run on the cluster. Uses bsub to enter jobs into
    the LSF queuing system. Extra arguments are passed to
    BsubCommand.build(). The return value is the integer LSF job ID.
    '''    
    pout = super(ClusterJobSubmitter, self).\
           submit_command(cmd,
                          path=self.conf.clusterpath,
                          *args, **kwargs)

    jobid_pattern = re.compile(r"Job\s+<(\d+)>\s+is\s+submitted\s+to")
    if not self.test_mode:
      for line in pout:
        matchobj = jobid_pattern.search(line)
        if matchobj:
          jobid = int(matchobj.group(1))
          LOGGER.info("LSF ID of submitted job: %d", jobid)
          return jobid

      raise ValueError("Unable to parse bsub output for job ID.")
    else:
      return 0 # Test mode only.
    
class ClusterJobRunner(RemoteJobRunner):
  
  '''Class to run jobs via simple SSH on the cluster.'''
  
  def __init__(self, remote_wdir=None, *args, **kwargs):
    
    self.conf        = Config()
    self.remote_host = self.conf.cluster
    self.remote_port = self.conf.clusterport
    self.remote_user = self.conf.clusteruser
    self.remote_wdir = self.conf.clusterworkdir if remote_wdir is None else remote_wdir
    try:
      self.transfer_host = self.conf.transferhost
    except AttributeError, _err:
      LOGGER.debug("Falling back to cluster host for transfer.")
      self.transfer_host = self.remote_host
    try:
      self.transfer_wdir = self.conf.transferdir
    except AttributeError, _err:
      LOGGER.debug("Falling back to cluster remote directory for transfer.")
      self.transfer_wdir = self.remote_wdir
      
    # Must call this *after* setting the remote host info.
    super(ClusterJobRunner, self).__init__(command_builder=SimpleCommand(),
                                           *args, **kwargs)
    
##############################################################################
class DesktopJobSubmitter(RemoteJobRunner):
  '''
  Class to run jobs on an alternative alignment host (typically a
  desktop computer with multiple cores). The job is run under nohup
  and nice -20, and a log file is created in the working directory on
  the remote host. The return value is a STDOUT filehandle.
  '''
  def __init__(self, *args, **kwargs):

    self.conf        = Config()
    self.remote_host = self.conf.althost
    self.remote_port = self.conf.althostport
    self.remote_user = self.conf.althostuser
    self.remote_wdir = self.conf.althostworkdir
    self.transfer_host = self.remote_host
    self.transfer_dir  = self.remote_wdir
    
    # Must call this *after* setting the remote host info.
    super(DesktopJobSubmitter, self).__init__(command_builder=NohupCommand(),
                                              *args, **kwargs)

  def submit_command(self, cmd, *args, **kwargs):
    '''
    Submit a command to run on the designated host desktop
    machine.
    '''
    return super(DesktopJobSubmitter, self).\
      submit_command(cmd,
                     path=self.conf.althostpath,
                     remote_wdir=self.remote_wdir,
                     *args, **kwargs)

##############################################################################
##############################################################################

class AlignmentManager(object):
  '''
  Parent class handling various functions required by scripts
  running BWA or other aligners across multiple parallel alignments
  (and merging their output) on the cluster.
  '''
  __slots__ = ('conf', 'samtools_prog', 'group', 'cleanup', 'loglevel',
               'split_read_count', 'bsub', 'merge_prog', 'logfile', 'debug', 'threads', 'sortthreads','postprocess')

  def __init__(self, merge_prog=None, cleanup=False, group=None,
               split_read_count=1000000,
               loglevel=logging.WARNING, debug=True):

    self.conf = Config()

    self.samtools_prog = 'samtools'
    
    # The merge_prog argument *must* be set when calling split_and_align.
    self.merge_prog    = merge_prog
    self.logfile       = self.conf.splitbwarunlog
    self.bsub          = JobSubmitter()
    self.split_read_count   = split_read_count
    self.threads       = int(self.conf.num_threads)
    self.sortthreads   = int(self.conf.num_threads_sort)
    
    self.cleanup       = cleanup
    self.group         = group
    self.debug         = debug    
    
    self._configure_logging(self.__class__.__name__, LOGGER)
    LOGGER.setLevel(loglevel)
    LOGGER.debug("merge_prog set to %s", self.merge_prog)

  def _configure_logging(self, name, logger=LOGGER):
    '''
    Configures the logs to be saved in self.logfile.
    '''

    if self.debug:
      logger.setLevel(logging.DEBUG) # log everything on debug level
    else:
      logger.setLevel(logging.INFO)
      
    # specify the format of the log file
    logfmt = "[%%(asctime)s] %s %%(levelname)s : %%(message)s" % (name,)
    fmt = logging.Formatter(logfmt)
    
    # Push stderr to logs; Note that any required StreamHandlers will
    # have been added in the child class.
    hdlr = logging.FileHandler(self.logfile)
    hdlr.setFormatter(fmt)
    hdlr.setLevel(min(logger.getEffectiveLevel(), logging.WARN))
    logger.addHandler(hdlr)

  def split_fq(self, fastq_fn):
    '''
    Splits fastq file to self.split_read_count reads per file using
    linux command line split for speed.
    In case compressed (gzip or bzip), the file will be uncompressed on fly.    
    '''
    LOGGER.info("Splitting %s (%d reads per split)", fastq_fn, (self.split_read_count*self.threads) )

    fastq_fn_suffix = fastq_fn + '-'
    if fastq_fn.endswith('.gz'):
      fastq_fn_suffix = fastq_fn.rstrip('.gz') + '-'
      cmd = 'gunzip -c %s | split -l %d - %s' % ( quote(fastq_fn), (self.split_read_count*4*self.threads), quote(fastq_fn_suffix) )
    elif fastq_fn.endswith('.bz2'):
      fastq_fn_suffix = fastq_fn.rstrip('.bz2') + '-'
      cmd = 'bzcat %s | split -l %d - %s' % ( quote(fastq_fn), (self.split_read_count*4*self.threads), quote(fastq_fn_suffix) )
    else:
      cmd = ("split -l %d %s %s" # split -l size file.fq prefix
             % (self.split_read_count*4*self.threads, quote(fastq_fn), quote(fastq_fn_suffix)))
    call_subprocess(cmd, shell=True,
                    tmpdir=self.conf.clusterworkdir,
                    path=self.conf.clusterpath)

    # glob will try and expand [, ], ? and *; we don't actually want that.
    # Here we quote them as per the glob docs in a character class [].
    bash_re  = re.compile(r'([?\[\]*])')
    fq_files = glob.glob(bash_re.sub(r'[\1]', fastq_fn_suffix) + "??")
    fq_files.sort()
    for fname in fq_files:
      LOGGER.debug("Created fastq file: '%s'", fname)
      if self.group != None:
        set_file_permissions(self.group, fname)

    # Clean up 
    if self.cleanup:
      os.unlink(fastq_fn)
      LOGGER.info("Unlinking fq file '%s'", fastq_fn)
    return fq_files

  def queue_merge(self, bam_files, depend, bam_fn, rcp_target, samplename=None):
    '''
    Submits samtools job for merging list of bam files to LSF cluster.
    '''

    ## ML: thread information is available in self.threads. However,
    
    assert( self.merge_prog is not None )
    input_files = " ".join(bam_files) # singly-bash-quoted
    LOGGER.debug("Entering queue_merge with input_files=%s", input_files)

    # The self.merge_prog command is assumed to be a python script
    # conforming to the set of arguments supported by
    # cs_runBwaWithSplit_Merge.py. We call 'python' here to pick up
    # the python in our path rather than the python in the merge_prog
    # script shebang line.
    cmd = ("python %s --loglevel %d"
           % (self.merge_prog, LOGGER.getEffectiveLevel()))
    if rcp_target:
      cmd += " --rcp %s" % (rcp_target,)
    if self.cleanup:
      cmd += " --cleanup"
    if self.group:
      cmd += " --group %s" % (self.group,)
    if samplename:
      cmd += " --sample %s" % (samplename,)
    cmd += " %s %s" % (bash_quote(bam_fn), input_files)

    LOGGER.info("preparing samtools merge on '%s'", input_files)
    LOGGER.debug(cmd)

    jobname = bam_files[0].split("_")[0] + "bam"
    jobid = self._submit_lsfjob(cmd, jobname, depend, mem=10000) # ML: Why such a large memory request?
    LOGGER.debug("got job id '%s'", jobid)

  def _submit_lsfjob(self, command, jobname, depend=None, sleep=0, mem=8000, threads=1):
    '''
    Executes command in LSF cluster.
    '''
    jobid = self.bsub.submit_command(command, jobname=jobname,
                                     depend_jobs=depend, mem=mem,
                                     path=self.conf.clusterpath,
                                     tmpdir=self.conf.clusterworkdir,
                                     queue=self.conf.clusterqueue,
                                     sleep=sleep, mincpus=threads)
    return '' if jobid is None else jobid

  def split_and_align(self, *args, **kwargs):
    '''
    Method used to launch the initial file splitting and bwa
    alignments. This class also submits a job dependent on the outputs
    of those alignments, which in turn merges the outputs to generate
    the final bam file.
    '''

    raise NotImplementedError()

  def _merge_files(self, output_fn, input_fns, samplename=None):
    '''
    Merges list of bam files.
    '''
    if len(input_fns) == 1:
      LOGGER.warn("Moving file: %s to %s", input_fns[0], output_fn)
      move(input_fns[0], output_fn)
    else:
      
      m1 = "%s_m1" % bash_quote(output_fn)
      m2 = "%s_m2" % bash_quote(output_fn)
      cmd = "mknod %s p && mknod %s p" % (m1, m2)
      # NB! samtools merge does not like naped pipe as output file. Hence the extra step of writing to stdout and cating to named pipe.
      ncmd = ("%s merge -u - %s > %s\n" # assumes sorted input bams.
              % (self.samtools_prog, " ".join([ bash_quote(x) for x in input_fns]), m1))
      # Prepare read group information
      (libcode, facility, lanenum, _pipeline) = parse_repository_filename(output_fn)
      if libcode is None:
        LOGGER.warn("Applying dummy read group information to output bam.")
        libcode  = os.path.basename(output_fn)
        facility = 'Unknown'
        lanenum  = 0
      if samplename is not None:
        sample = samplename
      else:
        samplename = libcode
      # Command for adding read groups
      ncmd += "picard AddOrReplaceReadGroups VALIDATION_STRINGENCY=SILENT COMPRESSION_LEVEL=0 INPUT=%s OUTPUT=%s RGLB=%s RGSM=%s RGCN=%s RGPU=%d RGPL=illumina\n" % (m1, m2, libcode, sample, facility, int(lanenum))
      # Command for compressing the file
      ncmd += "samtools view -b -@ %d %s > %s\n" % (self.threads, m2, bash_quote(output_fn))
      
      LOGGER.debug(ncmd)
      nfname = os.path.join(self.conf.clusterworkdir, "%s.nfile" % output_fn)
      ret = write_to_remote_file(ncmd, nfname, self.conf.clusteruser, self.conf.cluster)
      if ret > 0:
        LOGGER.error("Failed to create %s:%s" % (self.conf.cluster, nfname))
        sys.exit(1)
      cmd += " && npiper -i %s && rm %s %s" % (nfname, m1, m2)
      
      LOGGER.debug(cmd)
      pout = call_subprocess(cmd, shell=True,
                             tmpdir=self.conf.clusterworkdir,
                             path=self.conf.clusterpath)
    for line in pout:
      LOGGER.warn("SAMTOOLS: %s", line[:-1])
    if not os.path.isfile(output_fn):
      LOGGER.error("expected output file '%s' cannot be found.", output_fn)
      sys.exit("File access error.")
    if self.group:
      set_file_permissions(self.group, output_fn)
    if self.cleanup:
      for fname in input_fns:

        # Remove input bam file, as long as it's not the only one.
        if len(input_fns) > 1:
          LOGGER.info("Unlinking bam file '%s'", fname)
          os.unlink(fname)

  def copy_result(self, target, fname):
    '''
    Copies file to target location.
    '''
    qname = bash_quote(fname)    
    cmd = "scp -p -q %s %s" % (qname, target)
    LOGGER.debug(cmd)
    pout = call_subprocess(cmd, shell=True,
                           tmpdir=self.conf.clusterworkdir,
                           path=self.conf.clusterpath)
    count = 0
    for line in pout:
      LOGGER.warn("SCP: %s", line[:-1])
      count += 1
    if count > 0:
      LOGGER.error("Got errors from scp, quitting.")
      sys.exit("No files transferred.")
    flds = target.split(":")
    if len(flds) == 2: # there's a machine and path
      fn_base = os.path.basename(qname)
      cmd = "ssh %s touch %s/%s.done" % (flds[0], flds[1], bash_quote(fn_base))
      LOGGER.debug(cmd)
      call_subprocess(cmd, shell=True,
                      tmpdir=self.conf.clusterworkdir,
                      path=self.conf.clusterpath)
    if self.cleanup:
      os.unlink(fname)
    return

  def picard_cleanup(self, output_fn, input_fn, samplename=None):
    '''
    Run picard CleanSam, AddOrReplaceReadGroups,
    FixMateInformation. Note that this method relies on the presence
    of a wrapper shell script named 'picard' in the path.
    '''
    # if postprocess intermediate files will not be compressed,
    # we need to add additional step of bam compression in the end
    if not self.conf.compressintermediates:
      output_fn_final = output_fn
      output_fn = output_fn + "_uncompressed.bam"
    
    postproc = BamPostProcessor(input_fn=input_fn, output_fn=output_fn,
                                samplename=samplename,
                                tmpdir=self.conf.clusterworkdir, compress=self.conf.compressintermediates)

    # Run CleanSam
    call_subprocess(postproc.clean_sam(),
                    tmpdir=self.conf.clusterworkdir, path=self.conf.clusterpath)
    if self.cleanup:
      os.unlink(input_fn)
      
    # Run AddOrReplaceReadGroups
    call_subprocess(postproc.add_or_replace_read_groups(),
                    tmpdir=self.conf.clusterworkdir, path=self.conf.clusterpath)
    if self.cleanup:
      os.unlink(postproc.cleaned_fn)

    # Run FixMateInformation
    call_subprocess(postproc.fix_mate_information(),
                    tmpdir=self.conf.clusterworkdir, path=self.conf.clusterpath)
    if self.cleanup:
      os.unlink(postproc.rgadded_fn)
      
    if not self.conf.compressintermediates:
      cmd = "samtools view -b -@ %s %s > %s && rm %s" % (self.conf.num_threads, output_fn, output_fn_final, output_fn)
      call_subprocess(cmd,
                      tmpdir=self.conf.clusterworkdir, path=self.conf.clusterpath)
      if self.group:
        set_file_permissions(self.group, output_fn_final)
    else:
      if self.group:
        set_file_permissions(self.group, output_fn)

  def merge_alignments(self, input_fns, output_fn, rcp_target=None, samplename=None, postprocess=True):
    '''
    Method used to merge a set of bam files into a single output bam
    file.
    '''
    merge_fn = output_fn
    if postprocess:
      merge_fn = "%s_dirty.bam" % os.path.splitext(output_fn)[0]

#    output_fn_local = output_fn
#    if output_fn.startswith('/') or output_fn.startswith('~/'):
#      (opath, ofn) = os.path.split(output_fn)
#      output_fn_local = ofn

    LOGGER.info("merging '%s' into '%s'", ", ".join(input_fns), merge_fn)
    self._merge_files(merge_fn, input_fns, samplename)
    LOGGER.info("merged '%s' into '%s'", ", ".join(input_fns), merge_fn)

    # NB! Following coded out lines are deprecated code not needed any more.
    # The lines used to run picard CleanSam and FixMateInformation now taken care of earlier in the pipeline;
    # and picard AddReadGroup now part of merge_files.
    # 
    # LOGGER.info("running picard cleanup on '%s'", merge_fn)    
    # self.picard_cleanup(output_fn, merge_fn, samplename)
    # LOGGER.info("ran picard cleanup on '%s' creating '%s'", merge_fn, output_fn)
    
    if rcp_target:
      self.copy_result(rcp_target, output_fn)
      LOGGER.info("copied '%s' to '%s'", output_fn, rcp_target)

##############################################################################

class BwaAlignmentManager(AlignmentManager):
  '''
  Subclass of AlignmentManager implementing the bwa-specific
  components of our primary alignment pipeline.
  '''
  def __init__(self, nocc=None, bwa_algorithm=None, nosplit=False, *args, **kwargs):

    if bwa_algorithm is None:
      bwa_algorithm = 'aln'
    assert(bwa_algorithm in ('aln', 'mem'))
 
    super(BwaAlignmentManager, self).__init__(*args, **kwargs)

    # These are now identified by passing in self.conf.clusterpath to
    # the remote command.
    self.bwa_prog      = 'bwa'
    self.bwa_algorithm = bwa_algorithm

    self.split = True # By default, files are split for alignment with aligned files merged in the end.
    if nosplit:
      self.split = False
      
    if nocc:
      if self.bwa_algorithm == 'mem':
        raise StandardError("The nocc argument is not supported by bwa mem. Try bwa aln instead.")

      self.nocc = '-n %s' % (nocc,)

    else:
      self.nocc = ''
    
  def _run_pairedend_bwa_aln(self, fqname, fqname2, genome, jobtag, output_fn, samplename, delay=0, compress_output=False):
    '''
    Run bwa aln on paired-ended sequencing data.
    '''
    jobname1  = "%s_sai1" % (jobtag,)
    jobname2  = "%s_sai2" % (jobtag,)
    sai_file1 = "%s.sai" % fqname
    sai_file2 = "%s.sai" % fqname2

    jobname_bam = "%s_bam" % (jobtag,)
    outbambase  = bash_quote(fqname)
    outbam      = outbambase + ".bam"

    # Run bwa aln
    cmd1 = "%s aln -t %d %s %s > %s" % (self.bwa_prog, self.threads, genome,
                                  bash_quote(fqname),
                                  bash_quote(sai_file1))
    cmd2 = "%s aln -t %d %s %s > %s" % (self.bwa_prog, self.threads, genome,
                                  bash_quote(fqname2),
                                  bash_quote(sai_file2))

    readgroup = self._make_readgroup_string(output_fn, samplename)

    # Variables for picard tools
    # Some options are universal. Consider also adding QUIET=true, VERBOSITY=ERROR, TMP_DIR=DBCONF.tmpdir.
    # Though, no the picard commands below should not require write of any temporary files.
    picard_common_args = ('VALIDATION_STRINGENCY=SILENT', 'COMPRESSION_LEVEL=0')
    
    # Create named pipes for running commands in npiper
    p1 = "%s_p1" % fqname
    p2 = "%s_p2" % fqname
    p3 = "%s_p3" % fqname

    cmd3 = "mknod %s p && mknod %s p && mknod %s p && sleep 1" % (p1, p2, p3)
    
    # Run bwa sampe
    ncommands  = ("%s sampe %s %s %s %s %s %s"
             % (self.bwa_prog, self.nocc, genome, bash_quote(sai_file1),
                bash_quote(sai_file2), bash_quote(fqname), bash_quote(fqname2)))

    # Convert to bam
    ncommands += (" | %s view -b -S -u - > %s\n" % (self.samtools_prog, p1))
        
    # Run picard CleanSam
    ncommands += ("picard CleanSam INPUT=%s OUTPUT=%s %s\n" % (p1, p2, ' '.join(picard_common_args)))

    # Run picard FixMateInformation
    ncommands += ("picard FixMateInformation ASSUME_SORTED=true INPUT=%s OUTPUT=%s %s\n" % (p2, p3, ' '.join(picard_common_args)))

    # Run samtools sort
    # depending on number of threads, determine how much memory to allocate per thread.
    mem_string = ""
    mem = int(round(((int(self.conf.clustermem)-1000)/self.sortthreads),-2)) # leave 1000MB for other use.
    # if mem > 200:
    mem_string = " -m %dM" % mem      
    if compress_output:
      ncommands += ("%s sort -@ %d%s %s %s\n" % (self.samtools_prog, self.sortthreads, mem_string, p3, outbambase))
    else:
      ncommands += ("%s sort -l 0 -@ %d%s %s %s\n" % (self.samtools_prog, self.sortthreads, mem_string, p3, outbambase))

    
    # write ncommands to nfname
    nfname = os.path.join(self.conf.clusterworkdir, "%s.nfile" % fqname)
    ret = write_to_remote_file(ncommands, nfname, self.conf.clusteruser, self.conf.cluster)
    if ret > 0:
      LOGGER.error("Failed to create %s:%s" % (self.conf.cluster, nfname))
      sys.exit(1)

    cmd3 += " && npiper -i %s && rm %s %s %s %s %s %s %s" % (nfname, bash_quote(fqname), p1, p2, p3, nfname, sai_file1, sai_file2)

    LOGGER.info("starting bwa step1 on '%s'", fqname)
    
    jobid_sai1 = self._submit_lsfjob(cmd1, jobname1, sleep=delay, mem=int(self.conf.clustermem), threads=self.threads)
    LOGGER.debug("got job id '%s'", jobid_sai1)
    LOGGER.info("starting bwa step1 on '%s'", fqname2)
    jobid_sai2 = self._submit_lsfjob(cmd2, jobname2, sleep=delay, mem=int(self.conf.clustermem), threads=self.threads)
    LOGGER.debug("got job id '%s'", jobid_sai2)

    if jobid_sai1 and jobid_sai2:
      LOGGER.info("preparing bwa step2 on '%s'", fqname)
      jobid_bam = self._submit_lsfjob(cmd3, jobname_bam,
                                      (jobid_sai1, jobid_sai2), sleep=delay, mem=8000, threads=1)
      LOGGER.debug("got job id '%s'", jobid_bam)
    else:
      LOGGER.error("bjob submission for bwa step1 for '%s' or '%s' failed!",
                   fqname, fqname2)

    return(jobid_bam, outbam)

  def _run_singleend_bwa_aln(self, fqname, genome, jobtag, output_fn, samplename, delay=0, compress_output=False):
    '''
    Run bwa aln on single-ended sequencing data.
    '''
    jobname_bam = "%s_bam" % (jobtag,)
    outbambase  = bash_quote(fqname)
    outbam      = outbambase + ".bam"

    readgroup = self._make_readgroup_string(output_fn, samplename)

    # Variables for picard tools
    # Some options are universal. Consider also adding QUIET=true, VERBOSITY=ERROR, TMP_DIR=DBCONF.tmpdir.
    # Though, no the picard commands below should not require write of any temporary files.
    picard_common_args = ('VALIDATION_STRINGENCY=SILENT', 'COMPRESSION_LEVEL=0')
    
    # Create named pipes for running commands in npiper
    p1 = "%s_p1" % fqname
    p2 = "%s_p2" % fqname
    p3 = "%s_p3" % fqname

    cmd = "mknod %s p && mknod %s p && mknod %s p && sleep 1" % (p1, p2, p3)
    
    # Run bwa aln
    ncommands = ("%s aln -t %d %s %s" % (self.bwa_prog, self.threads, genome, bash_quote(fqname)))

    # Run bwa samse
    ncommands += (" | %s samse %s %s - %s" % (self.bwa_prog, self.nocc,
                                        genome, bash_quote(fqname)))
    # Convert to bam
    ncommands += (" | %s view -b -S -u - > %s\n" % (self.samtools_prog, p1))
    
    # Run picard CleanSam
    ncommands += ("picard CleanSam INPUT=%s OUTPUT=%s %s\n" % (p1, p2, ' '.join(picard_common_args)))

    # Run picard FixMateInformation
    ncommands += ("picard FixMateInformation ASSUME_SORTED=true INPUT=%s OUTPUT=%s %s\n" % (p2, p3, ' '.join(picard_common_args)))
    
    # depending on RAM available, we can allocate more for sorting
    mem_string = ""
    mem = int(round(((int(self.conf.clustermem)-10000)/self.sortthreads),-2)) # leave 10000MB for mapping and other use.
    # if less than 500MB of ram left, do not bother specifying
    # if mem > 500:
    mem_string = "-m %dM " % mem
    # Run samtools sort
    if compress_output:
      ncommands += ("%s sort -@ %d %s%s %s\n" % (self.samtools_prog, self.sortthreads, mem_string, p3, outbambase))
    else:
      ncommands += ("%s sort -l 0 -@ %d %s%s %s\n" % (self.samtools_prog, self.sortthreads, mem_string, p3, outbambase))
    
    # write ncommands to nfname
    nfname = os.path.join(self.conf.clusterworkdir, "%s.nfile" % fqname)
    ret = write_to_remote_file(ncommands, nfname, self.conf.clusteruser, self.conf.cluster)
    if ret > 0:
      LOGGER.error("Failed to create %s:%s" % (self.conf.cluster, nfname))
      sys.exit(1)

    cmd += " && npiper -i %s && rm %s %s %s %s %s" % (nfname, bash_quote(fqname), p1, p2, p3, nfname)
    
    LOGGER.info("starting bwa on '%s'", fqname)
    LOGGER.debug(cmd)
    jobid_bam = self._submit_lsfjob(cmd, jobname_bam, sleep=delay, mem=int(self.conf.clustermem), threads=self.threads)
    LOGGER.debug("got job id '%s'", jobid_bam)
    
    return(jobid_bam, outbam)

  def _make_readgroup_string(self, fname, samplename):

    # set readgroup information for the file
    (libcode, facility, lanenum, _pipeline) = parse_repository_filename(fname)
    if libcode is None:
      LOGGER.warn("Applying dummy read group information to output bam.")
      libcode  = os.path.basename(fname)
      facility = 'Unknown'
      lanenum  = 0
    sample = samplename if samplename is not None else libcode    

    return "\'@RG\tID:%d\tPL:%s\tPU:%d\tLB:%s\tSM:%s\tCN:%s\'" % (int(lanenum),'illumina',int(lanenum), libcode, sample, facility)
  
  def _run_bwa_mem(self, fqnames, genome, jobtag, output_fn, samplename, delay=0, compress_output=False):
    '''
    Run bwa mem on single- or paired-end sequencing data.
    '''
    
    assert(len(fqnames) in (1, 2))

    jobname_bam = "%s_bam" % (jobtag,)
    outbambase  = bash_quote(fqnames[0])
    outbam      = outbambase + ".bam"

    readgroup = ""
    # Check if readgroup information should be added by bwa
    if self.split is False:
      readgroup = "-R %s" % self._make_readgroup_string(output_fn, samplename)
      
    quoted_fqnames = " ".join([ bash_quote(fqn) for fqn in fqnames ])

    # Variables for picard tools
    # Some options are universal. Consider also adding QUIET=true, VERBOSITY=ERROR, TMP_DIR=DBCONF.tmpdir.
    # Though, no the picard commands below should not require write of any temporary files.
    picard_common_args = ('VALIDATION_STRINGENCY=SILENT', 'COMPRESSION_LEVEL=0')
          
    # Create named pipes for running commands in npiper
    p1 = "%s_p1" % fqnames[0]
    p2 = "%s_p2" % fqnames[0]
    p3 = "%s_p3" % fqnames[0]

    cmd = "mknod %s p && mknod %s p && mknod %s p && sleep 1" % (p1, p2, p3)
    
    # Run bwa mem
    ncommands = "%s mem %s -t %d %s %s" % (self.bwa_prog, readgroup, self.threads, genome, quoted_fqnames)

    # Run sam to bam conversion
    ncommands += (" | %s view -b -S -u - > %s\n" % (self.samtools_prog, p1))
    
    # Run picard CleanSam
    ncommands += ("picard CleanSam INPUT=%s OUTPUT=%s %s\n" % (p1, p2, ' '.join(picard_common_args)))

    # Run picard FixMateInformation
    ncommands += ("picard FixMateInformation ASSUME_SORTED=true INPUT=%s OUTPUT=%s %s\n" % (p2, p3, ' '.join(picard_common_args)))

    # depending on RAM available, allocate more per thread
    mem_string = ""
    mem = int(round(((int(self.conf.clustermem)-10000)/self.sortthreads),-2)) # leave 10000MB for mapping and other use.
    # if less than 500MB of ram per thread, do not bother specifying
    # if mem < 500:
    mem_string = "-m %dM " % mem
    
    # Run samtools sort
    if compress_output:
      ncommands += ("%s sort -@ %d %s%s %s\n" % (self.samtools_prog, self.sortthreads, mem_string, p3, outbambase))
    else:
      ncommands += ("%s sort -l 0 -@ %d %s%s %s\n" % (self.samtools_prog, self.sortthreads, mem_string, p3, outbambase))

    # write ncommands to nfname
    nfname = os.path.join(self.conf.clusterworkdir, "%s.nfile" % fqnames[0])
    ret = write_to_remote_file(ncommands, nfname, self.conf.clusteruser, self.conf.cluster)
    if ret > 0:
      LOGGER.error("Failed to create %s:%s" % (self.conf.cluster, nfname))
      sys.exit(1)

    # Run npiper and clean up temporary files.
    cmd += " && npiper -i %s && rm %s %s %s %s %s" % (nfname, p1, p2, p3, nfname, quoted_fqnames)
    
    LOGGER.info("Starting bwa mem on fastq files: %s", quoted_fqnames)
    LOGGER.debug(cmd)
    jobid_bam = self._submit_lsfjob(cmd, jobname_bam, sleep=delay, mem=int(self.conf.clustermem), threads=self.threads)
    LOGGER.debug("got job id '%s'", jobid_bam)

    return(jobid_bam, outbam)

  def run_bwas(self, genome, paired, fq_files, fq_files2, output_fn, samplename):
    '''
    Submits bwa alignment jobs for list of fq files to LSF cluster.
    '''
    job_ids = []
    out_names = []
    current = 0
    # splits the fq_file by underscore and returns first element which
    # in current name

    # Note that by default the bam file compression occurs in merge step.
    # However, if input fastq is not split and output bam is expected to be normal compressed bam, we need to compress
    # here as there won't be any merging/compressing in the end.
    compress_output=False
    if len(fq_files)==1:
      compress_output=True
    
    for fqname in fq_files:
      donumber = fqname.split("_")[0]
      jobtag   = "%s_%s" % (donumber, current)

      # Older bwa aln algorithm.
      if self.bwa_algorithm == 'aln':

        if paired:

          (jobid, outbam) = self._run_pairedend_bwa_aln(fqname, fq_files2[current],
                                                        genome, jobtag, output_fn, samplename, current, compress_output=compress_output)
        else:
        
          (jobid, outbam) = self._run_singleend_bwa_aln(fqname,
                                                        genome, jobtag, output_fn, samplename, current, compress_output=compress_output)

      # Newer bwa mem algorithm.
      elif self.bwa_algorithm == 'mem':

        fqnames = [ fqname ]
        if paired:
          fqnames.append(fq_files2[current])
          
        (jobid, outbam) = self._run_bwa_mem(fqnames, genome, jobtag, output_fn, samplename, compress_output)
        
      else:
        raise ValueError("BWA algorithm not recognised: %s" % self.bwa_algorithm)

      job_ids.append(jobid)
      out_names.append(outbam)
      current += 1

    return (job_ids, out_names)

  def _get_foreign_file(self, fn, host, attempts = 1, sleeptime = 2):
    '''Download file located in host'''

    # NOTE: We may still need to double-quote spaces the destination
    # passed to scp. Double-quoting brackets ([]) does not work, though.

    (path, fname) = os.path.split(fn)
    cmd = "rsync -a -e \"ssh -o StrictHostKeyChecking=no\" %s@%s:%s %s" % (self.conf.clusteruser, host, bash_quote(fn), bash_quote(fname) )

    LOGGER.info("Downloading %s" % (fname))
    
    start_time = time.time()
    while attempts > 0:
      subproc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
      (stdout, stderr) = subproc.communicate()
      retcode = subproc.wait()
      if stdout is not None:
        sys.stdout.write(stdout)
      if stderr is not None:
        sys.stderr.write(stderr)
      if retcode != 0:
        attempts -= 1
        if attempts <= 0:
          break
        LOGGER.warning(\
                       'Transfer failed with error code: %s\nTrying again (max %d times)',
                       stderr, attempts)
        time.sleep(sleeptime)
      else:
        break
        
    if retcode !=0:
      raise StandardError("ERROR. Failed to transfer file. Command was:\n   %s\n"
                  % (" ".join(cmd),) )
    
    time_diff = time.time() - start_time
    LOGGER.info("%s transferred in %d seconds." % (fname, time_diff) )
    
  def split_and_align(self, files, genome, samplename, rcp_target=None, lcp_target=None, fileshost=None):
    '''
    Method used to launch the initial file splitting and bwa
    alignments. This class also submits a job dependent on the outputs
    of those alignments, which in turn merges the outputs to generate
    the final bam file.
    '''
    #
    # fileshost - host where the target fastq files are located. If none, the files are expected to be local and accessible cluster wide.
    # keep_original = False - keep original files
    #

    # Transfer files in.
    local_files = []
    for fn in files:
      if fileshost is not None:
        # Throw an error in case files host is specified but no path to the files.
        self._get_foreign_file(fn, fileshost)
      (path, fname) = os.path.split(fn)
      local_files.append(fname)

    # Split file(s)
    if self.split:      
      assert( self.merge_prog is not None )
      fq_files = self.split_fq(local_files[0])
    else:
      fq_files = [local_files[0]]
    if len(local_files) == 2:
      if self.split:
        fq_files2 = self.split_fq(local_files[1])
      else:
        fq_files2 = [local_files[1]]
      paired = True
    elif len(local_files) == 1:
        fq_files2 = None
        paired = False
    else:
      LOGGER.error("Too many files specified.")
      sys.exit("Unexpected number of files passed to script.")
        
    # Construct output_fn
    bam_fn = "%s.bam" % make_bam_name_without_extension(local_files[0])
    if lcp_target is not None:
      if lcp_target.endswith('.bam'):        
        output_fn = os.path.basename(lcp_target)
      else:
        output_fn = os.path.join(lcp_target, bam_fn)
      bam_fn = output_fn
    else:
      if rcp_target is not None:
        if rcp_target.endswith('.bam'):
          output_fn = os.path.join(os.path.basename(rcp_target.split(':').pop()), bam_fn)
        else:
          output_fn = os.path.join(rcp_target.split(':').pop(), bam_fn)
      else:
        LOGGER.error("Neither lcp (%s) nor rcp (%s) has been defined!", lcp_target, rcp_target)
        output_fn = make_bam_name_without_extension(local_files[0])
    LOGGER.info("Saving mapping output to %s", output_fn)
        
    # Run bwa mapping jobs for each (pair of) file(s)
    (job_ids, bam_files) = self.run_bwas(genome, paired, fq_files, fq_files2, output_fn, samplename)

    # Note that 
    self.queue_merge(bam_files, job_ids, bam_fn, rcp_target, samplename)


##########################################################################
    
class TophatAlignmentManager(AlignmentManager):
  '''
  Subclass of AlignmentManager implementing the tophat2-specific
  components of our primary alignment pipeline.
  '''
  def __init__(self, *args, **kwargs):
    super(TophatAlignmentManager, self).__init__(*args, **kwargs)

    # These are now identified by passing in self.conf.clusterpath to
    # the remote command.
    self.tophat_prog   = 'tophat2'
    
  def run_tophat(self, genome, paired, fq_files, fq_files2):
    '''
    Submits tophat2 alignment jobs for list of fq files to LSF cluster.
    '''
    job_ids = []
    out_names = []
    current = 0

    # Tophat/bowtie requires the trailing .fa to be removed.
    genome = re.sub(r'\.fa$', '', genome)

    for fqname in fq_files:
      (donumber, facility, lanenum, _pipe) = parse_repository_filename(fqname)

      # Used as a job ID and also as an output directory, so we want
      # it fairly collision-resistant.
      jobname_bam = "%s_tophat" % fqname

      out = bash_quote(fqname + ".bam")
      out_names.append(out)

      # Run tophat2. The no-coverage-search option is required when
      # splitting the fastq file across multiple cluster nodes. The
      # fr-firststrand library type is the Odom lab default. We
      # use the -p option to ask for more threads; FIXME config option?
      cmd  = ("%s --no-coverage-search --library-type fr-firststrand -p 4 -o %s %s %s"
               % (self.tophat_prog, jobname_bam, genome, bash_quote(fqname)))
      if paired:
        cmd += " %s" % (bash_quote(fq_files2[current]),)

      # Merge the mapped and unmapped outputs, clean out unwanted
      # secondary alignments. Tophat2 sorts the output bams by default.
      strippedbam = "%s.partial" % out
      cmd += (" && %s view -b -F 0x100 -o %s %s"
               % (self.samtools_prog, strippedbam,
                   os.path.join(jobname_bam, 'accepted_hits.bam')))
      cmd += (" && %s merge %s %s %s"
               % (self.samtools_prog, out, strippedbam,
                  os.path.join(jobname_bam, 'unmapped.bam')))

      # Clean up
      cmd += (" && rm -r %s %s %s" % (jobname_bam, strippedbam, bash_quote(fqname)))
      if paired:
        cmd += " %s" % (bash_quote(fq_files2[current]),)
        
      LOGGER.info("starting tophat2 on '%s'", fqname)
      LOGGER.debug(cmd)
      jobid_bam = self._submit_lsfjob(cmd, jobname_bam, sleep=current)
      LOGGER.debug("got job id '%s'", jobid_bam)
      job_ids.append(jobid_bam)

      current += 1

    return (job_ids, out_names)
    
  def split_and_align(self, files, genome, samplename, rcp_target=None):
    '''
    Method used to launch the initial file splitting and bwa
    alignments. This class also submits a job dependent on the outputs
    of those alignments, which in turn merges the outputs to generate
    the final bam file.
    '''
    assert( self.merge_prog is not None )
    fq_files = self.split_fq(files[0])
    paired = False
    if len(files) == 2:
      fq_files2 = self.split_fq(files[1])
      paired = True
    elif len(files) == 1:
      fq_files2 = None
    else:
      LOGGER.error("Too many files specified.")
      sys.exit("Unexpected number of files passed to script.")
      
    (job_ids, bam_files) = self.run_tophat(genome, paired, fq_files, fq_files2)

    bam_fn = "%s.bam" % make_bam_name_without_extension(files[0])
    self.queue_merge(bam_files, job_ids, bam_fn, rcp_target, samplename)


##############################################################################
