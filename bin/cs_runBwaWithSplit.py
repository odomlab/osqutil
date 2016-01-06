#!/usr/bin/env python
#
# $Id$

""" Run BWA using Split to improve speed """
__author__ = "Margus Lukk"
__date__ = "05 Mar 2012"
__version__ = "0.2"
__credits__ = "Adjusted from cs_runMaqWithSplit written by Gordon Brown."

# Known bugs: 
# 

import sys # for processing command line arguments
import os # for miscellaneous operating system interfaces
import os.path # for manipulating path
import re # regular expressions module
import glob # module for listing filenames with wildcards
from distutils import spawn
from pipes import quote
import time

from osqutil.setup_logs import configure_logging
from logging import INFO, WARNING
LOGGER = configure_logging(level=INFO)
    
from osqutil.cluster import BwaAlignmentManager

##################  M A I N   P R O G R A M  ######################

if __name__ == '__main__':

  import argparse

  PARSER = argparse.ArgumentParser(
    description='Split a FASTQ file into chunks and align'
    + ' these chunks in parallel on the cluster.')

  PARSER.add_argument('genome', metavar='<genome>', type=str,
                      help='The genome against which to align.')

  PARSER.add_argument('files', metavar='<fastq file(s)>', type=str, nargs='+',
                      help='The fastq files to align.')

  PARSER.add_argument('--sample', type=str, dest='sample',
                      help='The sample name used to tag the output bam read group.')

  PARSER.add_argument('--loglevel', type=int, dest='loglevel', default=WARNING,
                      help='The level of logging.')

  PARSER.add_argument('--reads', type=int, dest='reads', default=1000000,
                      help='The number of reads in a split.')

  PARSER.add_argument('--rcp', type=str, dest='rcp',
                      help='Remote file copy (rcp) target.')

  PARSER.add_argument('--group', type=str, dest='group',
                      help='The user group for the files.')

  PARSER.add_argument('--cleanup', dest='cleanup', action='store_true',
                      help='Delete all temporary files.')

  PARSER.add_argument('--n_occ', dest='nocc', type=str,
                      help='Number of occurrences of non-unique reads to keep.')
# Currently unused:
#  PARSER.add_argument('--bwaoptions', type=str, dest='bwaoptions',
#                      help='BWA options as a string.')

  PARSER.add_argument('-d', '--debug', dest='debug', action='store_true',
                      help='Turn on debugging output.')

  ARGS = PARSER.parse_args()

  BSUB = BwaAlignmentManager(debug      = ARGS.debug,
                             cleanup    = ARGS.cleanup,
                             loglevel   = ARGS.loglevel,
                             split_read_count = ARGS.reads,
                             group      = ARGS.group,
                             nocc       = ARGS.nocc,
                             merge_prog = spawn.find_executable('cs_runBwaWithSplit_Merge.py',
                                                                path=os.environ['PATH']))

  BSUB.split_and_align(files      = ARGS.files,
                       genome     = ARGS.genome,
                       samplename = ARGS.sample,
                       rcp_target = ARGS.rcp)

  
