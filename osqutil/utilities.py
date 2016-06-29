#!/usr/bin/env python
#
# $Id$
#
# Written originally by Gordon Brown.
# Extensive edits and additions made by Tim Rayner and Margus Lukk.
#

'''A collection of frequently unrelated functions used elsewhere in the code.'''

import os
import os.path
import sys
import re
from tempfile import TemporaryFile
import stat
import grp
import gzip
import bz2
from contextlib import contextmanager
import hashlib
from subprocess import Popen, CalledProcessError, PIPE
from distutils import spawn
import threading
from .config import Config
from .setup_logs import configure_logging
from functools import wraps

###########################################################################
# N.B. we want no dependencies on the postgresql database in this
# module; it needs to be loadable on the cluster.
#

DBCONF = Config()
LOGGER = configure_logging('utilities')

###########################################################################
# Now for the rest of the utility functions...

def is_zipped(fname):
  '''
  Test whether a file is zipped or not, based on magic number with an
  additional check on file suffix. Assumes that the fname argument
  points directly to the file in question, and that the file is
  readable. Returns True/False accordingly.
  '''
  suff = os.path.splitext(fname)[1]
  if open(fname).read(2) == '\x1f\x8b': # gzipped file magic number.

    # Some files are effectively zipped but don't have a .gz
    # suffix. We model them in the repository as uncompressed, hence
    # we return False here. The main file type this affects is bam.
    if suff in ('.bam',):
      return False

    if suff != DBCONF.gzsuffix:
      LOGGER.warn("Gzipped file detected without %s suffix: %s",
                  DBCONF.gzsuffix, fname)

    return True
  else:
    if suff == DBCONF.gzsuffix:
      LOGGER.warn("Uncompressed file masquerading as gzipped: %s", fname)
    return False

def is_bzipped(fname):
  '''
  Test whether a file is bzipped or not, based on magic number with an
  additional check on file suffix. Assumes that the fname argument
  points directly to the file in question, and that the file is
  readable. Returns True/False accordingly.
  '''
  suff = os.path.splitext(fname)[1]
  if open(fname).read(3) == 'BZh': # bzip2 file magic number.

    if suff != '.bz2':
      LOGGER.warn("Bzipped file detected without '.bz2' suffix: %s",
                  fname)

    return True
  else:
    if suff == '.bz2':
      LOGGER.warn("Uncompressed file masquerading as bzipped: %s", fname)
    return False

def unzip_file(fname, dest=None, delete=True, overwrite=False):
  '''
  Unzip a file. If a destination filename is not supplied, we strip
  the suffix from the gzipped file, raising an error if the filename
  doesn't match our expectations.
  '''
  if not is_zipped(fname):
    raise ValueError("Attempted to unzip an already uncompressed file: %s"
                     % (fname,))

  # Derive the destination filename assuming a consistent filename
  # pattern.
  if dest is None:
    fnparts = os.path.splitext(fname)
    if fnparts[1] == DBCONF.gzsuffix:
      dest = os.path.splitext(fname)[0]
    else:
      raise ValueError("Unexpected gzipped file suffix: %s" % (fname,))

  # We refuse to overwrite existing output files.
  if os.path.exists(dest):
    if overwrite:
      os.unlink(dest)
    else:
      raise IOError("Gzip output file already exists; cannot continue: %s"
                    % (dest,))

  # We use external gzip where available
  LOGGER.info("Uncompressing gzipped file: %s", fname)
  if spawn.find_executable('gzip', path=DBCONF.hostpath):
    cmd = 'gzip -dc %s > %s' % (bash_quote(fname), bash_quote(dest))
    call_subprocess(cmd, shell=True, path=DBCONF.hostpath)

  else:

    # External gzip unavailable, so we use the (slower) gzip module.
    LOGGER.warning("Using python gzip module, which may be quite slow.")
    with open(dest, 'wb') as out_fd:
      with gzip.open(fname, 'rb') as gz_fd:
        for line in gz_fd:
          out_fd.write(line)

  if delete:
    os.unlink(fname)

  return dest

def rezip_file(fname, dest=None, delete=True, compresslevel=6, overwrite=False):
  '''
  Compress a file using gzip.
  '''
  if is_zipped(fname):
    raise ValueError("Trying to rezip an already-zipped file: %s" % (fname,))

  # Default gzip package compression level is 9; gzip executable default is 6.
  if not compresslevel in range(1,10):
    raise ValueError("Inappropriate compresslevel specified: %s" % (str(compresslevel),))

  if dest is None:
    dest = fname + DBCONF.gzsuffix

  # Check the gzipped file doesn't already exist (can cause gzip to
  # hang waiting for user confirmation).
  if os.path.exists(dest):
    if overwrite:
      os.unlink(dest)
    else:
      raise StandardError(
        "Output gzipped file already exists. Will not overwrite %s." % dest)

  # Again, using external gzip where available but falling back on the
  # (really quite slow) built-in gzip module where necessary.
  LOGGER.info("GZip compressing file: %s", fname)
  if spawn.find_executable('gzip', path=DBCONF.hostpath):
    cmd = 'gzip -%d -c %s > %s' % (compresslevel, bash_quote(fname), bash_quote(dest))
    call_subprocess(cmd, shell=True, path=DBCONF.hostpath)
  else:
    LOGGER.warning("Using python gzip module, which may be quite slow.")
    with gzip.open(dest, 'wb', compresslevel) as gz_fd:
      with open(fname, 'rb') as in_fd:
        for line in in_fd:
          gz_fd.write(line)

  if delete:
    os.unlink(fname)

  return dest

@contextmanager
def flexi_open(filename, *args, **kwargs):
  '''
  Simple context manager function to seamlessly handle gzipped and
  uncompressed files.
  '''
  if is_zipped(filename):
    handle = gzip.open(filename, *args, **kwargs)
  elif is_bzipped(filename):
    handle = bz2.BZ2File(filename, *args, **kwargs)
  else:
    handle = open(filename, *args, **kwargs)

  yield handle

  handle.close()

def _checksum_fileobj(fileobj, blocksize=65536):
  '''
  Use the hashlib.md5() function to calculate MD5 checksum on a file
  object, in a reasonably memory-efficient way.
  '''
  hasher = hashlib.md5()
  buf = fileobj.read(blocksize)
  while len(buf) > 0:
    hasher.update(buf)
    buf = fileobj.read(blocksize)

  return hasher.hexdigest()

def checksum_file(fname, unzip=True):
  '''
  Calculate the MD5 checksum for a file. Handles gzipped files by
  decompressing on the fly (i.e., the returned checksum is of the
  uncompressed data, to avoid gzip timestamps changing the MD5 sum).
  '''
  # FIXME consider piping from external gzip (where available) rather
  # than using gzip module?
  if unzip and is_zipped(fname):
    with gzip.open(fname, 'rb') as fileobj:
      md5 = _checksum_fileobj(fileobj)
  else:
    with open(fname, 'rb') as fileobj:
      md5 = _checksum_fileobj(fileobj)
  return md5

def parse_repository_filename(fname):
  '''
  Retrieve key information from a given filename.
  '''
  fname   = os.path.basename(fname)
  fnparts = os.path.splitext(fname)
  if fnparts[1] == DBCONF.gzsuffix:
    fname = fnparts[0]
  # N.B. don't add a bounding '$' as this doesn't match the whole
  # filename for e.g. *.mga.pdf. The terminal \. is important, in that
  # the MGA files will match but fastq will not. This match is dumped
  # as pipeline, which is semantically wrong but used consistently
  # elsewhere. FIXME to correctly return file type!
  name_pattern = re.compile(
    r"([a-zA-Z]+\d+)_.*_([A-Z]+)(\d+)(p[12])?(_chr21)?(\.[a-z]+)?\.")
  matchobj = name_pattern.match(fname)
  if matchobj:
    label = matchobj.group(1)
    fac = matchobj.group(2)
    lane = int(matchobj.group(3))
    if not matchobj.group(6):
      pipeline = 'chipseq' # default pipeline. Not elegant!
    else:
      pipeline = matchobj.group(6)[1:]
  else:
    LOGGER.warning("parse_repository_filename: failed to parse '%s'", fname)
    label = fac = lane = pipeline = None
  return (label, fac, lane, pipeline)

def get_filename_libcode(fname):
  '''Extract the library code from a given filename.'''

  # Takes first part of the filename up to the first underscore or period.
  name_pattern = re.compile(r"^([^\._]+)")
  matchobj = name_pattern.match(fname)
  return matchobj.group(1)

def set_file_permissions(group, path):
  '''
  Set a file group ownership, with appropriate read and write privileges.
  '''
  gid = grp.getgrnam(group).gr_gid
  try:
    os.chown(path, -1, gid)
    os.chmod(path,
             stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP)
  except OSError:
    LOGGER.warn("Failed to set ownership or permissions on '%s'."
                 + " Please fix manually.", path)

def bash_quote(string):
  '''Quote a string (e.g. a filename) to allow its use with bash and
  bash-related commands such as bsub, scp etc.'''

  # The following are all legal characters in a file path.
  bash_re = re.compile('(?=[^-+0-9a-zA-Z_,./\n])')
  return bash_re.sub('\\\\', string)

# Currently unused, we're keeping this in case it's useful in future.
def split_to_codes(string):
  # By Margus.
  # Date: 2012.10.29
  #
  # Todo: better error handling.

  """Splits string of comma separated DO numbers to a list of DO
  numbers. E.g. String: do234,do50-1,do500-do501,do201-198 would
  return do234, do50, do51, do500, do501, do198, do199, do200, do20"""
  codes = []
  string = string.lower()
  string = string.replace(' ', '')
  nums = string.split(',') # split by comma, continue working with substrings.
  for num in nums:
    # define some regexps
    rgx_complex = re.compile(r'^do.*\d+$')
    rgx_simple  = re.compile(r'^do\d+$')
    rgx_range   = re.compile(r'^do\d+-.*\d+$')
    rgx_digits  = re.compile(r'^\d+$')
    if re.match(rgx_complex, num):
      if re.match(rgx_simple, num):
        codes.append(num)
      elif re.match(rgx_range, num):
        splits = num.split('-')
        range_start = splits[0]
        range_end   = splits[1]
        if re.match(rgx_simple, range_start):
          # substring left of '-' is code
          num1 = range_start[2:]
          if re.match(rgx_simple, range_end):
            # substring right of '-' is code
            num2 = range_end[2:]
            if num2 >= num1:
              for i in range (int(num1), int(num2)+1):
                codes.append("do%d" % i)
            else:
              for i in range (int(num2), int(num1)+1):
                codes.append("do%d" % i)
          elif re.match(rgx_digits, range_end):
            # substring right of '-' is a number
            num2 = range_end
            length1 = len(num1)
            length2 = len(num2)
            if length1 <= length2:
              if num2 >= num1:
                for i in range (int(num1), int(num2)+1):
                  codes.append("do%d" % i)
              else:
                for i in range (int(num2), int(num1)+1):
                  codes.append("do%d" % i)
            else:
              num2rep = num1[:(length1-length2)] + num2
              if num2rep >= num1:
                for i in range (int(num1), int(num2rep)+1):
                  codes.append("do%d" % i)
              else:
                for i in range (int(num2rep), int(num1)+1):
                  codes.append("do%d" % i)
          else:
            # Dysfunctional substring! Generate error. Print what can be printed
            codes.append(range_start)
            # print "substr right from - did not match anything"
        else:
          # Dysfunctional substring! Generate error. Print what can be printed
          if range_end.match(rgx_simple):
            codes.append(range_end)
#           print "range_start did not match do*"
#     else:
#      # Dysfunctional substring! Generate an error. Print what can be printed
#      print " - not found in str"
#   else:
#    # Dysfunctional substring! Generate an error. Print what can be printed
#    print "matchobj match for ^do*.\d+$"
  return codes

def _write_stream_to_file(stream, fname):
  '''Simple function used internally to pipe a stream to a file.'''
  for data in stream:
    fname.write(data)

def call_subprocess(cmd, shell=False, tmpdir=DBCONF.tmpdir, path=None,
                    **kwargs):

  '''Generic wrapper around subprocess calls with handling of failed
  calls. This function starts threads to read stdout and stderr from
  the child process. In so doing it avoids deadlocking issues when
  reading large quantities of data from the stdout of the child
  process.'''

  # Credit to the maintainer of python-gnupg, Vinay Sajip, for the
  # original design of this function.

  # Set our PATH environmental var to point to the desired location.
  oldpath = os.environ['PATH']
  if path is not None:
    if type(path) is list:
      path = ":".join(path)
    os.environ['PATH'] = path
  else:
    LOGGER.warn("Subprocess calling external executable using undefined $PATH.")

  # We have **kwargs here mainly so we can support shell=True.
  kid = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=shell, **kwargs)

  stdoutfd = TemporaryFile(dir=tmpdir)
  stderrfd = TemporaryFile(dir=tmpdir)

  # We store streams in our own variables to avoid unexpected pitfalls
  # in how the subprocess object manages its attributes.

  # We disable this pylint error concerning missing Popen object
  # attributes because it's clearly bogus (subprocess is a core
  # module!).

  # pylint: disable=E1101
  stderr = kid.stderr
  err_read = threading.Thread(target=_write_stream_to_file,
                              args=(stderr, stderrfd))
  err_read.setDaemon(True)
  err_read.start()

  stdout = kid.stdout
  out_read = threading.Thread(target=_write_stream_to_file,
                              args=(stdout, stdoutfd))
  out_read.setDaemon(True)
  out_read.start()

  out_read.join()
  err_read.join()

  retcode = kid.wait()

  stderr.close()
  stdout.close()

  stdoutfd.seek(0, 0)

  os.environ['PATH'] = oldpath

  if retcode != 0:

    stderrfd.seek(0, 0)

    sys.stderr.write("\nSubprocess STDOUT:\n")
    for line in stdoutfd:
      sys.stderr.write("%s\n" % (line,))

    sys.stderr.write("\nSubprocess STDERR:\n")
    for line in stderrfd:
      sys.stderr.write("%s\n" % (line,))

    if type(cmd) == list:
      cmd = " ".join(cmd)

    raise CalledProcessError(kid.returncode, cmd)

  return stdoutfd

def munge_cruk_emails(emails):

  '''This is a (hopefully temporary) function needed to munge our new
  cruk.cam.ac.uk address lists to add in the old cancer.org.uk
  addresses as well. In principle this may be discardable with the
  move to a new LIMS.'''

  email_re = re.compile('@cruk.cam.ac.uk')
  return emails + [ email_re.sub('@cancer.org.uk', x) for x in emails ]

def read_file_to_key_value(fname, sep):
  """Reads text file (fname) to associated list. Key and value on the
  first and second column in each file are separated by sep."""
  keyvalue = {}
  with open(fname) as tabfh:
    for fline in tabfh:
      fline = fline.strip()   # remove surrounding whitespace
      try:
        key, value = fline.split(sep, 1)
      except ValueError:
        continue
      keyvalue[key] = value
  return keyvalue

# A central location for our default Fastq file naming scheme.
def build_incoming_fastq_name(sample_id, flowcell, flowlane, flowpair):

  '''Create a fastq filename according to the conventions used in
  cs_fetchFQ2.py.'''

  # N.B. This is typically called using the output of
  # parse_incoming_fastq_name, below.

  # Keep this in sync with the regex in parse_incoming_fastq_name.
  dst = "%s.%s.s_%s.r_%d.fq" % (sample_id, flowcell, flowlane, int(flowpair))
  return dst

def parse_incoming_fastq_name(fname, ext='.fq'):

  '''Parses the fastq filenames used by the pipeline during the
  initial download and demultiplexing process. The values matched by
  the regex are: library code (or list of library codes as stored in
  the LIMS); flowcell ID; flowcell lane; flow pair (1 or 2 at
  present).'''

  # Keep this in sync with the output of build_incoming_fastq_name.
  # Pattern altered to match the new filenames coming in from the
  # Genologics LIMS. Includes MiSeq naming.
  fq_re = re.compile(r'^([^\.]+)\.([\.\w-]+)\.s_(\d+)\.r_(\d+)%s$' % (ext,))
  matchobj = fq_re.match(fname)
  if matchobj is None:
    raise StandardError("Incoming file name structure not recognised: %s"
                        % fname)
  return (matchobj.group(*range(1, 5)))

def sanitize_samplename(samplename):
  '''
  Quick convenience function to remove potentially problematic
  characters from sample names (for use in bam file read groups, file
  names etc.).
  '''
  if samplename is None:
    return None
  sanity_re = re.compile(r'([ \/\(\);&|]+)')
  return(sanity_re.sub('_', samplename))

def determine_readlength(fastq):
  '''
  Guess the length of the reads in the fastq file. Assumes that the
  first read in the file is representative.
  '''
  # Currently just assumes that the second line is the first read, and
  # that it is representative.
  LOGGER.debug("Finding read length from fastq file %s...", fastq)
  rlen = None
  with flexi_open(fastq) as reader:
    for _num in range(2):
      line = reader.next()
    rlen = len(line.rstrip('\n'))

  return rlen

def memoize(func):
  '''
  Convenience function to memoize functions as necessary. May be of
  interest as a decorator for use with e.g. is_zipped(),
  checksum_file() etc. so that they can be called multiple times in
  defensively-written code but only actually read the file once.

  See
  https://technotroph.wordpress.com/2012/04/05/memoize-it-the-python-way/
  for discussion. Also note that python 3.2 and above have built-in
  memoization (functools.lru_cache).
  '''
  cache = {}
  @wraps(func)
  def wrap(*args):
    if args not in cache:
      cache[args] = func(*args)
    return cache[args]
  return wrap

class BamPostProcessor(object):

  __slots__ = ('input_fn', 'output_fn', 'cleaned_fn', 'rgadded_fn',
               'common_args', 'samplename', 'compress')

  def __init__(self, input_fn, output_fn, tmpdir=DBCONF.tmpdir, samplename=None, compress=False):

    self.input_fn    = input_fn
    self.output_fn = output_fn
    self.samplename  = samplename

    output_base = os.path.splitext(output_fn)[0]
    self.cleaned_fn  = "%s_cleaned.bam" % output_base
    self.rgadded_fn  = "%s_rg.bam" % output_base
    
    # Some options are universal. Consider also adding QUIET=true, VERBOSITY=ERROR
    self.common_args = ('VALIDATION_STRINGENCY=SILENT',
                        'TMP_DIR=%s' % tmpdir)
    # In case post processing intermediate files are expected to be uncompressed add COMPRESSION_LEVEL=0
    self.compress = compress
    if not compress:
      self.common_args = self.common_args + ('COMPRESSION_LEVEL=0')

  def clean_sam(self):

    # Run CleanSam
    cmd = ('picard', 'CleanSam',
           'INPUT=%s'  % self.input_fn,
           'OUTPUT=%s' % self.cleaned_fn) + self.common_args

    return cmd
  
  def add_or_replace_read_groups(self):

    (libcode, facility, lanenum, _pipeline) = parse_repository_filename(self.output_fn)
    if libcode is None:
      LOGGER.warn("Applying dummy read group information to output bam.")
      libcode  = os.path.basename(self.output_fn)
      facility = 'Unknown'
      lanenum  = 0

    sample = self.samplename if self.samplename is not None else libcode

    # Run AddOrReplaceReadGroups
    cmd = ('picard', 'AddOrReplaceReadGroups',
           'INPUT=%s'  % self.cleaned_fn,
           'OUTPUT=%s' % self.rgadded_fn,
           'RGLB=%s'   % libcode,
           'RGSM=%s'   % sample,
           'RGCN=%s'   % facility,
           'RGPU=%d'   % int(lanenum),
           'RGPL=illumina') + self.common_args

    return cmd

  def fix_mate_information(self):

    # Run FixMateInformation
    cmd = ('picard', 'FixMateInformation',
           'INPUT=%s'  % self.rgadded_fn,
           'OUTPUT=%s' % self.output_fn) + self.common_args
      
    return cmd
