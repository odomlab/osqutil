#!/usr/bin/env python
#
# $Id$

'''Code of uncertain function and provenance.'''

import sys
import re

from osqutil.setup_logs import configure_logging
LOGGER = configure_logging()

###############################################################################

def make_buckets(tags):
  offByOne = {}
  for tag in tags:
    for pos in range(0, len(tag)):
      for letter in "ACGT":
        if tag[pos] != letter:
          err = tag[0:pos] + letter + tag[pos+1:]
          offByOne[err] = tag
  return offByOne

def split_barcoded(codeStr, inFN):
  namePat = re.compile(r"^(.+)_(CRI\d\d)(_.+)?\.(\w+)$")
  nameMO = namePat.match(inFN)
  if not nameMO:
    print >> sys.stderr, "ERROR: failed to parse input name."
    sys.exit("Unexpected file naming convention.")
  base = nameMO.group(1)
  lane = nameMO.group(2)
  middle = nameMO.group(3)
  suff = nameMO.group(4)
  codeNames = codeStr.upper().split(",")
  codelen = len(codeNames[0])
  codes = {}
  counts = {}
  ob1codes = {}
  ob1counts = {}
  for code in codeNames:
    fname = "%s%s_%s_%s.%s" % (base, middle if middle else "",
                               code, lane, suff)
    fdesc = file(fname, "w")
    codes[code] = fdesc
    counts[code] = 0
    fname = "%s%s_%s_ob1_%s.%s" % (base, middle if middle else "",
                                   code, lane, suff)
    fdesc = file(fname, "w")
    ob1codes[code] = fdesc
    ob1counts[code] = 0
  fname = "%s%s_other_%s.%s" % (base, middle if middle else "",
                                lane, suff)
  fdesc = file(fname, "w")
  codes['other'] = fdesc
  counts['other'] = 0
  inFD = file(inFN)
  buckets = make_buckets(codeNames)
  for line in inFD:
    seq = line.split("\t")[8]
    code = seq[0:codelen].upper()
    if code in codes:
      codes[code].write(line)
      counts[code] += 1
    elif code in buckets:
      ob1codes[buckets[code]].write(line)
      ob1counts[buckets[code]] += 1
    else:
      codes['other'].write(line)
      counts['other'] += 1
  total = 0
  for code in codes:
    codes[code].close()
    total += counts[code]
    if code != 'other':
      print >> sys.stderr, "%s\t%d" % (code, counts[code])
  print >> sys.stderr, "other\t%d" % (counts['other'],)
  print "total\t%d" % (total,)

###############################################################################

if __name__ == '__main__':
  (CODESTR, INFN) = sys.argv[1:]
  split_barcoded(CODESTR, INFN)

