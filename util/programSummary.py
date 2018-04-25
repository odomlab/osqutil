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

# N.B. The interesting code was moved to its own library under libpy,
# so we don't need to put the bin directory on PYTHONPATH all the
# time.

from osqutil.progsum import ProgramSummary;

if __name__ == '__main__':
    import argparse

    PARSER = argparse.ArgumentParser(
    description='Finds program in file system. Reports program name, program path and program version (if available).')

    PARSER.add_argument('program', metavar='<program>', type=str,
                      help='Name of the program')
    ARGS = PARSER.parse_args()
    
    p = ProgramSummary(ARGS.program,path="ukulele",version="whoknows")
    print "Program: %s\nPath: %s\nVersion: %s" % (p.program,p.path,p.version)
