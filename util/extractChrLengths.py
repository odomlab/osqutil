#!/usr/bin/env python
#
# $Id$

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

