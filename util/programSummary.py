#!/usr/bin/env python

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
