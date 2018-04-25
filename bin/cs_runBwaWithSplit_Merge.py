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

""" Merge BWA generated files """
__author__ = "Margus Lukk"
__date__ = "05 Mar 2012"
__version__ = "0.2"
__credits__ = "Adjusted from cs_runMaqWithSplit_Merge written by Gordon Brown."

# Known bugs: 
# 

import sys
import os
import os.path
import re

from osqutil.setup_logs import configure_logging
from logging import INFO, WARNING
LOGGER = configure_logging(level=INFO)

from osqutil.cluster import BwaAlignmentManager

################################################################################

      
###############   M A I N   P R O G R A M   ############

if __name__ == '__main__':

  import argparse

  PARSER = argparse.ArgumentParser(
    description='Merge a set of aligned BAM files into one.')

  PARSER.add_argument('outfile', metavar='<output BAM filename>', type=str,
                      help='The name of the output BAM file.')

  PARSER.add_argument('infiles', metavar='<input BAM file(s)>',
                      type=str, nargs='+',
                      help='The BAM files to merge.')

  PARSER.add_argument('--sample', type=str, dest='sample',
                      help='The sample name used to tag the output bam read group.')

  PARSER.add_argument('--loglevel', type=int, dest='loglevel', default=WARNING,
                      help='The level of logging.')

  PARSER.add_argument('--rcp', type=str, dest='rcp',
                      help='Remote file copy (rcp) target.')

  PARSER.add_argument('--debug', dest='debug', action='store_true',
                      help='Switch the debugging info on.')
  
  PARSER.add_argument('--cleanup', dest='cleanup', action='store_true',
                      help='Delete all temporary files.')

  PARSER.add_argument('--postprocess', dest='postprocess', action='store_true',
                      help='Run picard based post process after bam merge.')
  
  PARSER.add_argument('--group', type=str, dest='group',
                      help='The user group for the files.')
  
  ARGS = PARSER.parse_args()

  # We allow the default bwa algorithm to be set here, since it does
  # not affect the merging step.
  BSUB = BwaAlignmentManager(debug      = ARGS.debug,
                             cleanup    = ARGS.cleanup,
                             loglevel   = ARGS.loglevel,
                             group      = ARGS.group)

  BSUB.merge_alignments(input_fns  = ARGS.infiles,
                        output_fn  = ARGS.outfile,
                        rcp_target = ARGS.rcp,
                        samplename = ARGS.sample,
                        postprocess= ARGS.postprocess)
  
