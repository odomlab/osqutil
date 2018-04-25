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

#
# Dependencies:
# - password free ssh to SERVER
# - hard coded default values for USER, SERVER, MAXJOBS, MAXPENDJOBS
#
# Known bugs:
#
#

import sys
import os
import re

"""Check the status of LSF cluster and number of pending and running jobs for a particular user. Besides returning general cluster and user level statistics, returns user status in cluster, either \'BUSY\' or \'OK\' depending if suggested maximum number of running and pending jobs has been exceeded. The script assumes password free ssh to the LSF cluster headnode."""

__author__ = "Margus Lukk"
__date__ = "28 Dec 2012"
__version__ = "0.1"
__credits__ = "Margus Lukk"

def get_lsfhosts(server):
    hosts=0
    runjobs=0
    # find number of active hosts and jobs running in cluster
    cmd ='ssh %s bhosts' % server
    pOut = os.popen(cmd,'r',1)
    for line in pOut:
        line = re.sub('\s+',' ',line)
        ## print "Line: \"%s\"" %line
        cols = line.split(' ')
        if cols[1] == "ok":
            hosts = hosts + int(cols[3])
            runjobs = runjobs + int(cols[5])
    pOut.close()
    return hosts,runjobs

def get_lsfuser_jobs(server,user):
    pendjobs = 0
    runjobs = 0

    cmd ='ssh %s bjobs -u %s 2>&1' % (server,user)
    pOut = os.popen(cmd,'r',1)
    for line in pOut:
        line = re.sub('\s+',' ',line)
        cols = line.split(' ')
        if cols[2] == "PEND":
            pendjobs = pendjobs + 1 
        if cols[2] == "RUN":
            runjobs = runjobs + 1
    pOut.close()
    return pendjobs,runjobs
    
def get_cluster_summary(server,user,maxrun,maxpend):
    (hosts,runjobs) = get_lsfhosts(server)
    (upendjobs,urunjobs) = get_lsfuser_jobs(server,user)        
    status = "BUSY"
    if urunjobs < maxrun and upendjobs < maxpend:
        status = "OK"
    return (hosts,runjobs,upendjobs,urunjobs,status)

if __name__ == '__main__':
    import argparse

    USER = 'fnc-odompipe'
    SERVER = 'uk-cri-lcst01.crnet.org'
    MAXJOBS = 100
    MAXPENDJOBS = 5

    PARSER = argparse.ArgumentParser(
    description='Check the status of LSF cluster and number of pending and running jobs for a particular user.'
    + 'Besides returning general cluster and user level statistics, returns user status in cluster, '
    + 'either \'BUSY\' or \'OK\' depending if suggested maximum number of running and pending jobs has been exceeded.'
    + 'The script assumes password free ssh to the LSF cluster headnode.')

    PARSER.add_argument('--server', type=str, dest='server', default=SERVER,
    help='Hostname for LSF cluster headnode')

    PARSER.add_argument('--user', type=str, dest='user', default=USER,
    help='Username in LSF cluster')
    
    PARSER.add_argument('--maxjobs', type=str, dest='maxjobs', default=MAXJOBS,
    help='Number of running jobs considered to be OK for the user. Default = %s' % MAXJOBS)

    PARSER.add_argument('--maxpendjobs', type=str, dest='maxpendjobs', default=MAXPENDJOBS,
    help='Number of pending jobs considered to be OK for the user. Default = %s' % MAXPENDJOBS)

    ARGS = PARSER.parse_args()
    
    print "---------"
    print "User: %s" % ARGS.user
    (hosts,runjobs,upendjobs,urunjobs,status) = get_cluster_summary(ARGS.server,ARGS.user,ARGS.maxjobs,ARGS.maxpendjobs)
    print "Available nodes: %s" % hosts
    print "Occupied nodes: %s" % runjobs
    print "%s pending: %s" % (ARGS.user,upendjobs)
    print "%s running: %s" % (ARGS.user,urunjobs)
    print "Status: %s" % status
    print "---------"
