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

"""Checks alignment (bed) file in repository for regions aligned over
chromosome end. This is of particular importance when uploading bed
files to the UCSC genome browser."""

__author__ = "Margus Lukk"
__date__ = "19 Nov 2012"
__version__ = "0.2"
__credits__ = "Margus Lukk"

# Basic core of the old cs_trimBedToGenome.py script refactored to
# remove all pipeline library dependencies so that it can be run by
# lab members on any server with python.

import sys
import os
import os.path
import tempfile

# Copied directly from osqutil.utilities to remove dependency on that library.
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

def trim_bed_for_overhanging_regions(infile, fnchrlen, outfile,
                                     truncate=False, add_header=False, workdir='.'):

  """Removes regions that have been aligned over the edge of the
  chromosome from the bed file. Arguments: infile - bed file name,
  fnchrlen - file containing chromosome length information, fnout -
  output filename."""

  if type(outfile) == 'str':  
    if not os.path.exists(outfile):
      sys.exit("Error: file not found: %s" % (outfile,))
    outfile = open(outfile)
  
  # Read in chr lengths.
  chrLenDict = read_file_to_key_value(fnchrlen, "\t")
  # Find lines in bed file where chr coordinate too big.
  print ("Searching %s for overhanging regions. Saving results to %s"
         % (infile, outfile.name))

  skipped   = 0
  truncated = 0

  if add_header:

    # Add a basic track header for UCSC upload. Future versions may
    # want to allow for greater customisation.
    knownexts = dict(bgr='bedGraph',
                     bed='bed')
    parts = os.path.splitext(infile)
    if len(parts) != 2:
      raise ValueError("Filename does not appear to have an extension: %s" % infile)
    basename = parts[0]
    filetype = knownexts.get(parts[1][1:], 'unknown')
    outfile.write(("""track type=%s name="%s" description="%s" visibility=2"""
                   + """ color=255,0,0 windowingFunction=maximum alwaysZero=ON\n""")
                  % (filetype, basename, basename))

  for fline in open(infile):

    # Skip any input header line. A better fix would detect this
    # earlier and skip the add_header step above (FIXME).
    if fline[:5] == 'track':
      outfile.write(fline)
      continue

    fcols  = fline.split("\t")
    chrlen = chrLenDict.get(fcols[0], None)

    if chrlen is not None:

      # Coords are okay
      if int(fcols[1]) < int(chrlen) and int(fcols[2]) < int(chrlen):
        outfile.write(fline)

      # Skip an overhanging interval
      elif not truncate:
        skipped += 1

      # Truncate an overhanging interval if it's appropriate.
      else:
        if int(fcols[1]) < int(chrlen) and int(fcols[2]) > int(chrlen):
          fcols[2] = chrlen
          outfile.write("\t".join(fcols))
          truncated += 1
        elif int(fcols[1]) > int(chrlen) and int(fcols[2]) < int(chrlen):
          fcols[1] = chrlen
          outfile.write("\t".join(fcols))
          truncated += 1
        else:
          skipped += 1
          
  return (skipped, truncated)

def trim_bed_local(fname, chrlength, workdir='.', truncate=False, add_header=False):

  '''Trim a bed file according to the chromosome lengths in the
  chrlength file.'''

  if not os.path.exists(workdir):
    os.makedirs(workdir)
    
  fnbase   = os.path.basename(fname)
  fnout    = os.path.join(workdir, fnbase)
  fnouttmp = tempfile.NamedTemporaryFile(dir=workdir, delete=False)

  (skipped, truncated) = trim_bed_for_overhanging_regions(fname, chrlength,
                                                          fnouttmp, truncate=truncate,
                                                          add_header=add_header)

  if skipped or truncated:
    print "%s overhanging regions removed." % skipped
    print "%s overhanging regions truncated." % truncated
  else:
    print "No overhanging regions found!"

  # This keeps the header (if added) even in cases where no regions
  # were altered.
  print "Renaming trimmed output file to %s." % fnout
  os.rename(fnouttmp.name, fnout)

################## M A I N ########################

if __name__ == '__main__':

  import argparse

  PARSER = argparse.ArgumentParser(
    description='Trims bed files to either remove or truncate intervals'
    + ' which overhang the ends of chromosomes and/or scaffolds.')

  PARSER.add_argument(
      '-b', '--bed', dest='bedfile', required=True, type=str,
      help='The name of the bed file to trim.')

  PARSER.add_argument(
      '-c', '--chrlength', dest='chrlength',
      required=True, type=str,
      help='A file containing chromosome/scaffold lengths. This should'
      + ' be generated using the fetchChromSizes program available from UCSC.')

  PARSER.add_argument(
      '-d', '--dir', dest='dir',
      required=False, type=str, default='.',
      help='A directory where to save the output file.'
      + ' The default value corresponds to the current working'
      + ' directory. Can be used together with options -i and -g.')

  PARSER.add_argument(
      '-t', '--truncate', dest='truncate', action='store_true',
      help='Rather than removing an overhanging interval entirely,'
      + ' trim the edge so that it falls within the genome boundaries.'
      + ' This is useful for processing peak caller output.')

  PARSER.add_argument(
      '--add-header', dest='header', action='store_true',
      help='Add a header to the output file to allow it, for example,'
      + ' to be uploaded directly to the UCSC genome browser.')

  ARGS = PARSER.parse_args()

  if not os.path.exists(ARGS.bedfile):
    print "\nFile %s is missing or not accessible. Quitting.\n" % ARGS.bedfile
    PARSER.print_help()
    sys.exit(1)
    
  trim_bed_local(ARGS.bedfile, chrlength=ARGS.chrlength,
                 workdir=ARGS.dir, truncate=ARGS.truncate, add_header=ARGS.header)
