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

    cmd = super(SbatchCommand, self).build(cmd, *args, **kwargs)
    
    # Add information about environment in front of the command.
    envstr = " ".join([ "%s=%s" % (key, val) for key, val in environ.iteritems() ])
    cmd = envstr + " " + cmd

    # In some cases it is beneficial to wait couple of seconds before the job is executed
    # As the job may be executed immediately, we add little wait before the execution of the rest of the command.
    if sleep > 0:
      cmd = ('sleep %d && ' % sleep) + cmd

    # uuid.uuid1() creates unique string based on hostname and time.
    ltmpdir = tempfile.gettempdir()
    slurmfile = str(uuid.uuid1())
    # if cluster log dir has not been specified, overwrite locally with clusterstdoutdir
    if clusterlogdir is None:
      clusterlogdir = self.conf.clusterstdoutdir
    fslurmfile = os.path.join(clusterlogdir, slurmfile)

    # Create sbatch bash script to a long string
    cmd_text = '#!/bin/bash\n'
    if jobname is not None:
      cmd_text += '#SBATCH -J %s\n' % jobname # where darwinjob is the jobname
    #cmd_text += '#SBATCH -A CHANGEME\n' # In university cluster paid version this argument is important! I.e. which project should be charged.
    
    # A safety net in case min or max nr of cores gets muddled up. An
    # explicit error is preferred in such cases, so that we can see
    # what to fix.
    if mincpus > maxcpus:
      maxcpus = mincpus
      LOGGER.info("mincpus (%d) is greater than maxcpus (%d). Maxcpus was made equal to mincpus!" % (mincpus, maxcpus))
    cmd_text += '#SBATCH -N 1\n' # Make sure that all cores are in one node
    cmd_text += '#SBATCH --mincpus=%d\n' % mincpus # Specify the number of CPU cores we need. Using --ntasks 1 and --cpus-per-task=mincpus should do the same job.
    cmd_text += '#SBATCH --mail-type=NONE\n' # never receive mail
    if queue is None:
      cmd_text += '#SBATCH -p %s\n' % self.conf.clusterqueue # Queue where the job is sent.
    else:
      cmd_text += '#SBATCH -p %s\n' % queue # Queue where the job is sent.
    cmd_text += '#SBATCH --open-mode=append\n' # record information about job re-sceduling
    if auto_requeue:
      cmd_text += '#SBATCH --requeue\n' # requeue job in case node dies etc.
    else:
      cmd_text += '#SBATCH --no-requeue\n' # do not requeue the job
    cmd_text += '#SBATCH --mem %s\n' % mem # memory in MB
    # cmd_text += '#SBATCH -t 0-%s\n' % time_limit # Note that time_limit is a string in format of hh:mm
    cmd_text += '#SBATCH -o %s/%%j.stdout\n' % clusterlogdir # File to which STDOUT will be written
    cmd_text += '#SBATCH -e %s/%%j.stderr\n' % clusterlogdir # File to which STDERR will be written
    if depend_jobs is not None:
      dependencies = '#SBATCH --dependency=aftercorr' # execute job after all corresponding jobs
      for djob in depend_jobs:
        dependencies += ':%s' % djob
      cmd_text += '%s\n' % dependencies
    # Following (two) lines are not necessarily needed but suggested by University Darwin cluster for record keeping in scheduler log files.
    cmd_text += 'numnodes=$SLURM_JOB_NUM_NODES\n'
    cmd_text += 'numtasks=$SLURM_NTASKS\n'
    cmd_text += 'hostname=`hostname`\n'
    cmd_text += 'workdir=\"$SLURM_SUBMIT_DIR\"\n'
    # This is the place where the actual command we want to execute is added to the script.
    cmd_text += 'CMD=\"%s\"\n' % cmd
    # Change dir to work directory.
    cmd_text += 'cd %s\n' % self.conf.clusterworkdir
    cmd_text += 'echo -e \"Changed directory to `pwd`.\n\"\n'
    cmd_text += 'JOBID=$SLURM_JOB_ID\n'
    cmd_text += 'echo -e \"JobID: $JOBID\n======\"\n'
    cmd_text += 'echo "Job start time: `date`"\n'
    cmd_text += 'echo \"Executed in node: $hostname\"\n'
    cmd_text += 'echo \"CPU info: `cat /proc/cpuinfo | grep name | uniq | tr -s \' \' | cut -f2 -d:`\"\n'
    cmd_text += 'echo \"Current directory: `pwd`\"\n'
    # cmd_text += 'echo -e \"\nnumtasks=$numtasks, numnodes=$numnodes\"\n'
    cmd_text += 'echo -e \"Number of cores requested: min=%d, max=%d\"\n' % (mincpus, maxcpus)
    cmd_text += 'echo -e \"Number of nodes received: $numnodes\"\n'
    cmd_text += 'echo -e \"\nExecuting command:\n==================\n$CMD\n\"\n'
    cmd_text += 'mv %s %s/$SLURM_JOB_ID.sh\n' % (fslurmfile, clusterlogdir)
    cmd_text += 'eval $CMD\n\n'
    cmd_text += 'echo "Job end time: `date`"\n'
    # Write sbatch file to cluster
    try:
      sshkey = self.conf.clustersshkey
    except AttributeError, _err:
      sshkey = None
    write_to_remote_file(cmd_text, fslurmfile, self.conf.clusteruser,
                         self.conf.cluster, append=False, sshkey=sshkey)
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
      if provider[:3].lower() == 'san' or provider[:3].lower() == 'ebi':
        resources = ('select[mem>%d] ' % mem) + resources
        memreq    = '-M %d' % mem
    except AttributeError:
      pass

    # A safety net in case min or max nr of cores gets muddled up. An
    # explicit error is preferred in such cases, so that we can see
    # what to fix.
    if mincpus > maxcpus:
      raise ValueError("mincpus (%d) is greater than maxcpus (%d). Surely some error?" % (mincpus, maxcpus))

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
  __slots__ = ('test_mode', 'conf', 'command_builder')

  def __init__(self, test_mode=False, command_builder=None, *args, **kwargs):
    self.test_mode = test_mode
    if test_mode:
      LOGGER.setLevel(logging.DEBUG)
    else:
      LOGGER.setLevel(logging.INFO)

    self.conf = Config()

    self.command_builder = SimpleCommand() \
        if command_builder is None else command_builder

  def run_command(self, cmd, tmpdir=None, path=None, command_builder=None, *args, **kwargs):

    if command_builder:
      cmd = command_builder.build(cmd, *args, **kwargs)
    else:
      cmd = self.command_builder.build(cmd, *args, **kwargs)

    if path is None:
      path = self.conf.hostpath

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
    conf = Config() # self.conf is set in superclass __init__
    
    if conf.clustertype == 'SLURM':
      super(JobSubmitter, self).__init__(command_builder=SbatchCommand(),
                                         *args, **kwargs)
    elif conf.clustertype == 'LSF':
      super(JobSubmitter, self).__init__(command_builder=BsubCommand(),
                                         *args, **kwargs)
    else:
      LOGGER.error("Unknown cluster type '%s'. Exiting.", conf.clustertype)
      sys.exit(1)

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
    jobid_pattern = None
    if self.conf.clustertype == 'LSF':
      jobid_pattern = re.compile(r"Job\s+<(\d+)>\s+is\s+submitted\s+to")
    elif self.conf.clustertype == 'SLURM':
      jobid_pattern = re.compile(r"Submitted batch job (\d+)")
    else:
      LOGGER.error("Unknown cluster type '%s'. Exiting.", self.conf.clustertype)
      sys.exit(1)

    for line in pout:
      matchobj = jobid_pattern.search(line)
      if matchobj:
        jobid = int(matchobj.group(1))
        LOGGER.info("ID of submitted job: %d", jobid)
        return jobid
      
    raise ValueError("Unable to parse job scheduler output for job ID.")

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

    # Allow for custom ssh key specification in our config.
    sshcmd = "ssh"
    try:
      sshkey = self.conf.clustersshkey
      sshcmd += ' -i %s' % sshkey
    except AttributeError, _err:
      pass

    cmd = ("%s -p %s %s@%s \"source /etc/profile; cd %s && %s %s\""
           % (sshcmd,
              str(self.remote_port),
              self.remote_user,
              self.remote_host,
              wdir,
              pathdef,
              re.sub(r'"', r'\"', cmd)))
    LOGGER.debug(cmd)
    if not self.test_mode:
      return call_subprocess(cmd, shell=True, path=self.conf.hostpath)
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
      try:
        sshkey = self.conf.clustersshkey
        cmdbits += ['-i', sshkey]
      except AttributeError, _err:
        pass

      cmdbits += ['-q', bash_quote(fromfn),
                  "%s@%s:%s" % (self.remote_user,
                                self.transfer_host,
                                quote(destfile))]
      cmd = " ".join(cmdbits)

      LOGGER.debug(cmd)
      if not self.test_mode:
        call_subprocess(cmd, shell=True, path=self.conf.hostpath)

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

    conf        = Config() # self.conf is set in superclass __init__
    self.remote_host = conf.cluster
    self.remote_port = conf.clusterport
    self.remote_user = conf.clusteruser
    self.remote_wdir = conf.clusterworkdir if remote_wdir is None else remote_wdir
    try:
      self.transfer_host = conf.transferhost
    except AttributeError, _err:
      LOGGER.debug("Falling back to cluster host for transfer.")
      self.transfer_host = self.remote_host
    try:
      self.transfer_wdir = conf.transferdir
    except AttributeError, _err:
      LOGGER.debug("Falling back to cluster remote directory for transfer.")
      self.transfer_wdir = self.remote_wdir

    # Must call this *after* setting the remote host info.
    if conf.clustertype == 'SLURM':
      super(ClusterJobSubmitter, self).__init__(command_builder=SbatchCommand(),
                                                *args, **kwargs)
    elif conf.clustertype == 'LSF':
      super(ClusterJobSubmitter, self).__init__(command_builder=BsubCommand(),
                                                *args, **kwargs)
    else:
      LOGGER.error("Unknown cluster type '%s'. Exiting.", conf.clustertype)
      sys.exit(1)

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

    jobid_pattern = None
    if self.conf.clustertype == 'LSF':
      jobid_pattern = re.compile(r"Job\s+<(\d+)>\s+is\s+submitted\s+to")
    elif self.conf.clustertype == 'SLURM':
      jobid_pattern = re.compile(r"Submitted batch job (\d+)")
    else:
      LOGGER.error("Unknown cluster type '%s'. Exiting.", self.conf.clustertype)
      sys.exit(1)
    
    if not self.test_mode:
      for line in pout:
        matchobj = jobid_pattern.search(line)
        if matchobj:
          jobid = int(matchobj.group(1))
          LOGGER.info("ID of submitted job: %d", jobid)
          return jobid

      raise ValueError("Unable to parse bsub output for job ID.")
    else:
      return 0 # Test mode only.

class ClusterJobRunner(RemoteJobRunner):

  '''Class to run jobs via simple SSH on the cluster.'''

  def __init__(self, remote_wdir=None, *args, **kwargs):

    conf        = Config() # self.conf is set in superclass __init__
    self.remote_host = conf.cluster
    self.remote_port = conf.clusterport
    self.remote_user = conf.clusteruser
    self.remote_wdir = conf.clusterworkdir if remote_wdir is None else remote_wdir
    try:
      self.transfer_host = conf.transferhost
    except AttributeError, _err:
      LOGGER.debug("Falling back to cluster host for transfer.")
      self.transfer_host = self.remote_host
    try:
      self.transfer_wdir = conf.transferdir
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

    conf        = Config() # self.conf is set in superclass __init__
    self.remote_host = conf.althost
    self.remote_port = conf.althostport
    self.remote_user = conf.althostuser
    self.remote_wdir = conf.althostworkdir
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
               'split_read_count', 'bsub', 'merge_prog', 'logfile', 'debug')

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
    '''
    LOGGER.debug("splitting fq file %s to %s per file ", fastq_fn, self.split_read_count)

    fastq_fn_suffix = fastq_fn + '-'
    cmd = ("split -l %s %s %s" # split -l size file.fq prefix
           % (self.split_read_count*4, quote(fastq_fn), quote(fastq_fn_suffix)))
    call_subprocess(cmd, shell=True,
                   tmpdir=self.conf.clusterworkdir,
                   path=self.conf.clusterpath)

    # glob will try and expand [, ], ? and *; we don't actually want
    # that.  Here we quote them as per the glob docs in a character
    # class []. We then run a second search to be sure we're getting all
    # the files (large files split into *-zaaa and so on).
    bash_re  = re.compile(r'([?\[\]*])')
    fq_files =  glob.glob(bash_re.sub(r'[\1]', fastq_fn_suffix) + "??")
    fq_files += glob.glob(bash_re.sub(r'[\1]', fastq_fn_suffix) + "????")
    fq_files.sort()
    for fname in fq_files:
      LOGGER.debug("Created fastq file: '%s'", fname)
      if self.group != None:
        set_file_permissions(self.group, fname)
    if self.cleanup:
      os.unlink(fastq_fn)
      LOGGER.info("Unlinking fq file '%s'", fastq_fn)
    return fq_files

  def queue_merge(self, bam_files, depend, bam_fn, rcp_target, samplename=None):
    '''
    Submits samtools job for merging list of bam files to LSF cluster.
    '''
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
    jobid = self._submit_lsfjob(cmd, jobname, depend, mem=12000)
    LOGGER.debug("got job id '%s'", jobid)

  def _submit_lsfjob(self, command, jobname, depend=None, sleep=0, mem=8000):
    '''
    Executes command in LSF cluster.
    '''
    jobid = self.bsub.submit_command(command, jobname=jobname,
                                     depend_jobs=depend, mem=mem,
                                     path=self.conf.clusterpath,
                                     tmpdir=self.conf.clusterworkdir,
                                     queue=self.conf.clusterqueue,
                                     sleep=sleep)
    return '' if jobid is None else jobid

  def split_and_align(self, *args, **kwargs):
    '''
    Method used to launch the initial file splitting and bwa
    alignments. This class also submits a job dependent on the outputs
    of those alignments, which in turn merges the outputs to generate
    the final bam file.
    '''
    raise NotImplementedError()

  def _merge_files(self, output_fn, input_fns):
    '''
    Merges list of bam files.
    '''
    if len(input_fns) == 1:
      LOGGER.warn("renaming file: %s", input_fns[0])
      move(input_fns[0], output_fn)
    else:
      cmd = ("%s merge %s %s" # assumes sorted input bams.
             % (self.samtools_prog,
                bash_quote(output_fn),
                " ".join([ bash_quote(x) for x in input_fns])))
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
    postproc = BamPostProcessor(input_fn=input_fn, output_fn=output_fn,
                                samplename=samplename,
                                tmpdir=self.conf.clusterworkdir)

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
    
    if self.group:
      set_file_permissions(self.group, output_fn)

  def merge_alignments(self, input_fns, output_fn, rcp_target=None, samplename=None):
    '''
    Method used to merge a set of bam files into a single output bam
    file.
    '''
    merge_fn = "%s_dirty.bam" % os.path.splitext(output_fn)[0]

    LOGGER.info("merging '%s' into '%s'", ", ".join(input_fns), merge_fn)
    self._merge_files(merge_fn, input_fns)
    LOGGER.info("merged '%s' into '%s'", ", ".join(input_fns), merge_fn)

    LOGGER.info("running picard cleanup on '%s'", merge_fn)
    self.picard_cleanup(output_fn, merge_fn, samplename)
    LOGGER.info("ran picard cleanup on '%s' creating '%s'", merge_fn, output_fn)

    if rcp_target:
      self.copy_result(rcp_target, output_fn)
      LOGGER.info("copied '%s' to '%s'", output_fn, rcp_target)

##############################################################################

class BwaAlignmentManager(AlignmentManager):
  '''
  Subclass of AlignmentManager implementing the bwa-specific
  components of our primary alignment pipeline.
  '''
  def __init__(self, nocc=None, bwa_algorithm=None, *args, **kwargs):

    if bwa_algorithm is None:
      bwa_algorithm = 'aln'
    assert(bwa_algorithm in ('aln', 'mem'))

    super(BwaAlignmentManager, self).__init__(*args, **kwargs)

    # These are now identified by passing in self.conf.clusterpath to
    # the remote command.
    self.bwa_prog      = 'bwa'
    self.bwa_algorithm = bwa_algorithm

    if nocc:

      if self.bwa_algorithm == 'mem':
        raise StandardError("The nocc argument is not supported by bwa mem. Try bwa aln instead.")

      self.nocc = '-n %s' % (nocc,)

    else:
      self.nocc = ''
    
  def _run_pairedend_bwa_aln(self, fqname, fqname2, genome, jobtag, delay=0):
    '''
    Run bwa aln on paired-ended sequencing data.
    '''
    jobname1  = "%s_sai1" % (jobtag,)
    jobname2  = "%s_sai2" % (jobtag,)
    sai_file1 = "%s.sai" % fqname
    sai_file2 = "%s.sai" % fqname2

    jobname_bam = "%s_bam" % (jobtag,)
    outbam      = bash_quote(fqname + ".bam")

    # Run bwa aln
    cmd1 = "%s aln %s %s > %s" % (self.bwa_prog, genome,
                                  bash_quote(fqname),
                                  bash_quote(sai_file1))
    cmd2 = "%s aln %s %s > %s" % (self.bwa_prog, genome,
                                  bash_quote(fqname2),
                                  bash_quote(sai_file2))

    # Run bwa sampe
    cmd3  = ("%s sampe %s %s %s %s %s %s"
             % (self.bwa_prog, self.nocc, genome, bash_quote(sai_file1),
                bash_quote(sai_file2), bash_quote(fqname), bash_quote(fqname2)))

    # Convert to bam
    cmd3 += (" | %s view -b -S -u - > %s.unsorted" % (self.samtools_prog, outbam))

    # Sort the bam
    cmd3 += (" && %s sort %s.unsorted %s" % (self.samtools_prog, outbam, bash_quote(fqname)))

    # Cleanup
    cmd3 += (" && rm %s %s %s %s %s.unsorted"
             % (bash_quote(sai_file1), bash_quote(sai_file2),
                bash_quote(fqname), bash_quote(fqname2), outbam))

    LOGGER.info("starting bwa step1 on '%s'", fqname)
    jobid_sai1 = self._submit_lsfjob(cmd1, jobname1, sleep=delay)
    LOGGER.debug("got job id '%s'", jobid_sai1)
    LOGGER.info("starting bwa step1 on '%s'", fqname2)
    jobid_sai2 = self._submit_lsfjob(cmd2, jobname2, sleep=delay)
    LOGGER.debug("got job id '%s'", jobid_sai2)

    if jobid_sai1 and jobid_sai2:
      LOGGER.info("preparing bwa step2 on '%s'", fqname)
      jobid_bam = self._submit_lsfjob(cmd3, jobname_bam,
                                      (jobid_sai1, jobid_sai2), sleep=delay)
      LOGGER.debug("got job id '%s'", jobid_bam)
    else:
      LOGGER.error("bjob submission for bwa step1 for '%s' or '%s' failed!",
                   fqname, fqname2)

    return(jobid_bam, outbam)

  def _run_singleend_bwa_aln(self, fqname, genome, jobtag, delay=0):
    '''
    Run bwa aln on single-ended sequencing data.
    '''
    jobname_bam = "%s_bam" % (jobtag,)
    outbam      = bash_quote(fqname + ".bam")

    # Run bwa aln
    cmd  = ("%s aln %s %s" % (self.bwa_prog, genome, bash_quote(fqname)))

    # Run bwa samse
    cmd += (" | %s samse %s %s - %s" % (self.bwa_prog, self.nocc,
                                        genome, bash_quote(fqname)))
    # Convert to bam
    cmd += (" | %s view -b -S -u - > %s.unsorted" % (self.samtools_prog, outbam))

    # Sort the output bam
    cmd += (" && %s sort %s.unsorted %s" % (self.samtools_prog,
                                            outbam, bash_quote(fqname)))
    # Clean up
    cmd += (" && rm %s %s.unsorted" % (bash_quote(fqname), outbam))
        
    LOGGER.info("starting bwa on '%s'", fqname)
    LOGGER.debug(cmd)
    jobid_bam = self._submit_lsfjob(cmd, jobname_bam, sleep=delay)
    LOGGER.debug("got job id '%s'", jobid_bam)

    return(jobid_bam, outbam)

  def _run_bwa_mem(self, fqnames, genome, jobtag, delay=0):
    '''
    Run bwa mem on single- or paired-end sequencing data.
    '''
    assert(len(fqnames) in (1, 2))

    jobname_bam = "%s_bam" % (jobtag,)
    outbambase  = bash_quote(fqnames[0])
    outbam      = outbambase + ".bam"

    # Run bwa mem
    quoted_fqnames = " ".join([ bash_quote(fqn) for fqn in fqnames ])
    cmd  = ("%s mem %s %s" % (self.bwa_prog, genome, quoted_fqnames))

    # Convert to bam
    cmd += (" | %s view -b -S -u - > %s.unsorted" % (self.samtools_prog, outbam))

    # Sort the output bam
    cmd += (" && %s sort %s.unsorted %s" % (self.samtools_prog,
                                            outbam, outbambase))
    # Clean up
    cmd += (" && rm %s %s.unsorted" % (quoted_fqnames, outbam))
        
    LOGGER.info("Starting bwa mem on fastq files: %s", quoted_fqnames)
    LOGGER.debug(cmd)
    jobid_bam = self._submit_lsfjob(cmd, jobname_bam, sleep=delay)
    LOGGER.debug("got job id '%s'", jobid_bam)

    return(jobid_bam, outbam)

  def run_bwas(self, genome, paired, fq_files, fq_files2):
    '''
    Submits bwa alignment jobs for list of fq files to LSF cluster.
    '''
    job_ids = []
    out_names = []
    current = 0
    # splits the fq_file by underscore and returns first element which
    # in current name
    for fqname in fq_files:
      donumber = fqname.split("_")[0]
      jobtag   = "%s_%s" % (donumber, current)

      # Older bwa aln algorithm.
      if self.bwa_algorithm == 'aln':

        if paired:

          (jobid, outbam) = self._run_pairedend_bwa_aln(fqname, fq_files2[current],
                                                        genome, jobtag, current)
        else:
        
          (jobid, outbam) = self._run_singleend_bwa_aln(fqname,
                                                        genome, jobtag, current)

      # Newer bwa mem algorithm.
      elif self.bwa_algorithm == 'mem':

        fqnames = [ fqname ]
        if paired:
          fqnames.append(fq_files2[current])
          
        (jobid, outbam) = self._run_bwa_mem(fqnames, genome, jobtag, current)
        
      else:
        raise ValueError("BWA algorithm not recognised: %s" % self.bwa_algorithm)

      job_ids.append(jobid)
      out_names.append(outbam)
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

    (job_ids, bam_files) = self.run_bwas(genome, paired, fq_files, fq_files2)

    bam_fn = "%s.bam" % make_bam_name_without_extension(files[0])
    self.queue_merge(bam_files, job_ids, bam_fn, rcp_target, samplename)

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
