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

'''Quick script to extract a tab-delimited file of chromosome lengths
from a genome fasta file. The output should be usable with
trimBedToGenome.py'''

import sys
import re

def seq_lengths(fasta):

  '''Extract a dict of sequence lengths from a fasta file.'''

  results = {}
  tag_re  = re.compile(r'^\s*>(.*)')
  with open(fasta) as inseq:

    current_chr = None
    current_len = 0
    
    for fline in inseq:
      matchobj = tag_re.match(fline)
      if matchobj:
        sys.stderr.write('.')
        if current_chr:
          results[current_chr] = current_len
          current_len = 0
        current_chr = matchobj.group(1)
      else:
        current_len += len(fline)

    # End of file has no next tag_re match.
    if current_chr:
      results[current_chr] = current_len
      current_len = 0

    sys.stderr.write("\n")
      
    return results

if __name__ == '__main__':

  import argparse

  PARSER = argparse.ArgumentParser(
    description='Extract sequence lengths from fasta.')

  PARSER.add_argument('-f', '--fasta', dest='fasta', type=str, required=True,
                      help='The input fasta file.')

  ARGS = PARSER.parse_args()

  LENGTHS = seq_lengths(ARGS.fasta)

  for key, value in LENGTHS.items():
    print "\t".join((key, str(value)))

