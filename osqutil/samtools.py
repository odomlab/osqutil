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

'''Code used to interface with the samtools package. This is used
e.g. to convert between bam and bed file formats.'''

import os
import sys
import re

from .utilities import read_file_to_key_value, call_subprocess
from .config import Config
from .setup_logs import configure_logging

CONFIG = Config()
LOGGER = configure_logging('samtools')

def count_bam_reads(bam):
  '''
  Quick function to count the total number of reads in a bam
  file. We discard supplementary alignments, introduced by bwa mem and
  other more recent alignment algorithms.
  '''
  LOGGER.info("Checking number of reads in bam file %s", bam)
  cmd  = (CONFIG.read_sorter, 'flagstat', bam)
  pout = call_subprocess(cmd, path=CONFIG.hostpath)
  numreads = int(pout.readline().split()[0])
  for line in pout:
    bits = [ x.strip() for x in line.split() ]

    # Catch annoying samtools typo and any future correction.
    if bits[3] in ('supplimentary', 'supplementary'):
      numreads -= int(bits[0])

  return numreads

def identify_bwa_algorithm(bam):
  '''
  Function which looks at the bam file header to try and identify
  whether bwa aln or bwa mem was used. Requires a recent version of
  bwa. Therefore if the appropriate annotation is not found, it is
  assumed that bwa aln was used. Function will likely raise an error if a
  non-bwa bam file is tested.
  '''
  LOGGER.info("Checking BWA algorithm bam file %s", bam)
  cmd  = (CONFIG.read_sorter, 'view', '-H', bam)
  pout = call_subprocess(cmd, path=CONFIG.hostpath)

  algo = 'aln'

  for line in pout:
    bits = [ x.strip() for x in line.split("\t") ]
    if bits[0] == '@PG':
      fields = dict( field.split(':', 1) for field in bits
                     if not re.match('^@', field) )
      if 'CL' in fields:
        (prog, algo, rest) = fields['CL'].split(" ", 2)
        if prog != 'bwa':
          raise ValueError("Expected bwa aligner, found %s" % prog)
        if algo == 'samse': # aln is implied
          algo = 'aln'
        break # Just take the first PG group

  return algo

class BamToBedConverter(object):

  '''Class holding code which uses samtools to convert from bam to bed
  file format. Objects have the following optional attributes: tc1,
  flag indicating that the tc1 genome was used in the alignment and
  that the output should be split into chr21+everything else;
  chrom_sizes, a string designating a chromosome sizes file as
  downloaded from UCSC using fetchChromSizes. The latter option may be
  used to remove overhanging reads which fall outside the range of the
  chromosome coordinates.'''

  __slots__ = ('tc1', 'chrom_sizes')

  def __init__(self, tc1=False, chrom_sizes=None):
    self.tc1 = tc1
    if chrom_sizes is not None:
      self.chrom_sizes = read_file_to_key_value(chrom_sizes, "\t")
    else:
      # Deactivate the overhanging read filter.
      self.chrom_sizes = None

  def convert(self, in_fn, out_fn):

    '''Actually run the conversion. Takes an input and output
    filename, and returns a list of output filenames (which may differ
    from that specified if the tc1 genome has been used in the
    alignment).'''

    # Start parsing the sam file with the help of converting bam to
    # sam with samtools view
    LOGGER.info("Converting bam file %s to bed file %s", in_fn, out_fn)

    out_fns = []

    # FIXME this should be trivial to implement via pysam.
    cmd = [ 'samtools', 'view', in_fn ]

    # call_subprocess seems to function approximately as fast
    # as a straight pipe from subprocess.Popen, despite its writing
    # interim data to disk. Bear this in mind if we run into
    # performance issues, though.
    in_fd   = call_subprocess(cmd, bufsize=1, path=CONFIG.hostpath)
    out_fd1 = open(out_fn, "w")

    out_fn2 = None
    out_fd2 = None

    if(self.tc1):
      (base, ext) = os.path.splitext(out_fn)
      out_fn2 = base + "_chr21" + ext
      LOGGER.info("Saving chr21 data into %s", out_fn2)
      out_fd2 = open(out_fn2, "w")

    count = 0
    mapped = 0
    unmapped = 0
    skipped = 0

    for line in in_fd:

      # The core of the samtools view output parser.
      if line[0] == '@':
        continue
      count += 1
      flds = line.split()
      flag = int(flds[1])
      if flag & 0x0004:
        unmapped += 1
        continue # read is unmapped
      if flag & 0x0800:
        continue # supplementary alignment; discard.
      mapped += 1
      strand = '+'
      if flag & 0x0010:
        strand = '-'

      left  = int(flds[3])-1
      right = left + len(flds[9])

      # Check for overhanging reads and drop them.
      chrlen = None
      if self.chrom_sizes is not None:
        chrlen = self.chrom_sizes.get(flds[2], None)
        if chrlen is None or left > int(chrlen) or right > int(chrlen):
          skipped = skipped + 1
          continue

      # Actually write out the line.
      if self.tc1 and flds[2] == "chr21":
        out_fd2.write("%s\t%d\t%d\t%s\t%s\t%s\n" % (flds[2], left, right,
                                                    flds[0], flds[4], strand))
      else:
        out_fd1.write("%s\t%d\t%d\t%s\t%s\t%s\n" % (flds[2], left, right,
                                                    flds[0], flds[4], strand))

      # Some user-friendly feedback.
      if count % 100000 == 0:
        sys.stderr.write("%d %d\r" % (count, mapped))

    if self.chrom_sizes is not None:
      LOGGER.info("%d overhanging reads removed.", skipped)

    in_fd.close()
    out_fd1.close()
    out_fns.append(out_fn)
    if(self.tc1):
      out_fns.append(out_fn2)
    LOGGER.info("read %d, wrote %d (%d unmapped)\n", count, mapped, unmapped)

    return out_fns
