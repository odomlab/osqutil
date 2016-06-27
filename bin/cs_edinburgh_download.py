#!/usr/bin/env python
#
# $Id$

""" Run Data Download from Edinburgh """
__author__ = "Margus Lukk"
__date__ = "10 June 2016"
__version__ = "0.1"

import sys
import os
import re
from subprocess import Popen, PIPE

# set up logger
from osqpipe.pipeline.setup_logs import configure_logging
from logging import INFO
LOGGER = configure_logging(level=INFO)

# set up cluster job submitter
from osqutil.cluster import ClusterJobSubmitter

# Import some functions from utilities package
from osqutil.utilities import call_subprocess, _checksum_fileobj # Note that for _checksum_fileobj this may not work as _ indicates that the function is not part of API!

# set up config
DBCONF = Config()

## In the heart of the download code lies following:
# 1. Given [USER ID], construct links to all files for this [userid]. NB! What edinburgh calls userid will be in our case donumber.
# 2. Download file and download md5sum
# 3. Compare file and md5sum, if not same, go back to 2. Keep track how many attempts file was downloaded (limit to 10.)
# 4. Set file on diks .done
# 5. Get lane info for lane and flowcell. If none, register flowcell.
# 6. Set flowcell status 'downloaded'
## There are multiple entry points to this code

## Note that the code depends on following variables defined in config:
# auser - aspera username (to be determined.
# ahost - host of the aspera server in Ediburng (edgen-dt.rdf.ac.uk)
# apport - aspera port for TCP communication
# aoport - aspera port for UDP communication
# arate - aspera download rate. E.g. 500M = 500Mbit/s
# athreads - number of parallel downloads to execute

# CHECKLIST BEFORE THIS CODE CAN BE TAKEN TO PRODUCTION
# 0. Check that userid will be the same as donumber
# 1. Has aspera username (auser) in config file been set correctly?
# 2. Are aspera ports correct
# 3. Is aspera project alone enough to build correct links to the files?
# 4. Download files for one [userid]
# 5. If previous was successful, execute download for 2 [userids] in a file configuring number of athreads to be 1.
# 6. Test further downloads with different number of athreads
# 7. Take code to production and download the rest of the data with maximum number of athreads that the bandwith can take

def read_credentials(credentials_file):
    '''Reads credentials from a file to memory.'''
    # Assumes following file structure
    # username\tuser str
    # password\tpassword str
    # 
    credentials = {'username':None, 'password':None}
    fh = open(credentials_file,'r')
    for line in fh:
        cols = line.split('\t')
        if cols[0] in self.credentials:
            credentials[cols[0]] = cols[1]
    for key in credentials:
        if credentials is None:
            sys.exit('Ill formated credentials file. No value for \'%s\'!\n\n' % key)
    return credentials

def compute_md5(fn):

    md5 = None
    with open(fn, 'rb') as fileobj:
        md5 = _checksum_fileobj(fileobj)
    return md5

import re
from subprocess import Popen, PIPE

def parse_read_header(fn):
    '''Extracts flowcell from the name of the first read in the file. Assumes either uncompressed, gzipped or bzipped fastq input file'''
    
    # Construct command for extraction of flowcell from file
    cmd = 'cat %s' % fn
    if fn.endswith('.gz'):
        cmd = 'zcat %s' % fn
    if fn.endswith('.bz2'):
        cmd = 'bzcat %s' % fn
    cmd = cmd + ' | head -n 1'
    
    # Run command
    subproc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    (stdout, stderr) = subproc.communicate()
    retcode = subproc.wait()
    
    # stderr is likely to contain error trown by zcat or bzcat due to head existing after reading only one line.
    # e.g. 'gzip: stdout: Broken pipe' or 'bzcat: I/O or other error, bailing out. Possible reason follows.'
    # Assume Illumina default read header:
    m = re.search('^@([0-9A-Za-z]+):\d+:([0-9A-Za-z]+):(\d):.*$', stdout)
    if m:
        machine = m.group(1)
        flowcell = m.group(2)
        flowlane = m.group(3)
        if len(flowcell) >= 7:
            return (machine,flowcell,flowlane)
    sys.exit("Failed extracting flowcell information from file '%s'\n" % fn)




# 1. Given [USER ID], construct links to all files

class ed_downloader(object):
    def __init__(self, maxattempts=5):

        # Set maximum number of attempts to download any single file
        self.maxattempts = maxattempts
        
        # Get following values from conf file        
        self.auser = DBCONF.auser
        self.ahost = DBCONF.ahost
        self.aPport = DBCONF.apport
        self.aoport = DBCONF.aoport
        self.arate = DBCONF.arate
        self.athreads = DBCONF.athreads
        self.destination = DBCONF.incoming
        self.failedcommands = DBCONF.faileddownloads
        
        # Load aspera credentials
        self.credentials = read_credentials(DBCONF.acredfile)
        os.environ['ASPERA_SCP_PASS'] = self.credentials['password']        
        
    def ed_download(ifile, project):
        '''Reads userids in ifile and downloads files for each userid'''

        # Set some variables for submitting the download jobs to cluster.
        nr_of_downloadthreads = 1
        submitter = ClusterJobSubmitter()

        # Open the file containing donumbers/userids/library.codes one on each line.        
        with open(self.ifile, 'rb') as fh:

            # Set some variables for threaded downloading.
            # We will try to control for the number of threads by
            # setting new download jobs to depend on complete of previous download jobs
            # The assumption is that in average the run time of all download threads will be the same.
            tnr = 0
            jobids = []
            newids = []
            
            for line in fh:
                userid = line.rstrip('\n')
                # ed_get_files_by_userid(userid, project)

                ## Prepare download command for for submision to the cluster
                cmd = 'cs_edinburgh_download.py -p %s -l %s' % (project, userid)
                if jobids:
                    jobid = submitter.submit_command(cmd=cmd, mem=1000, auto_requeue=False, depend_jobs=jobids[tnr])
                else:
                    jobid = submitter.submit_command(cmd=cmd, mem=1000, auto_requeue=False)
                newids.append(jobid)
                tnr += 1
                if tnr == self.athreads:
                    jobids = newids
                    newids = []
                    tnr = 0

    def ed_get_files_by_userid(userid, project):
        '''Downloads all files for the userid'''
        # For each userid in Edinburgh Genomics (i.e. in our case library.code / do-number), the files available are:
        # [userid]_R1.fastq.gz
        # [userid]_R1.fastq.gz.md5
        # [userid]_R2.fastq.gz
        # [userid]_R2.fastq.gz.md5
        # [userid]_R1_fastqc.html
        # [userid]_R1_fastqc.html.md5
        # [userid]_R2_fastqc.html
        # [userid]_R2_fastqc.html.md5
        
        fprefixes = ['_R1.fastq.gz', '_R2.fastq.gz', '_R1_fastqc.html', '_R2_fastqc.html']
        fqfile1 = "%s_%s" % (userid, fprefixes[0])
        failed = False
        
        for fprefix in fprefixes:
            fname = "%s_%s" % (userid, fprefix)
            
            attempts = 0
            while ed_get_file(fname, project) is None:
                attempts += 1
                if attempts == self.maxattempts:
                    # Record failed command
                    cmd  = 'echo cs_edinburgh_download.py -l %s -p %s >> %s' % (userid, project, self.failedcommands)
                    call_subprocess(cmd, shell=True, path=self.destination)
                    failed = True
        if failed == False:
            # Get flowcell and machine info from read header of fastq file
            (machine, flowcell, flowlane) = parse_read_header(fqfile1)
            # 5. Set flowcell status 'downloaded' or 'failed download'
            cmd = 'communicateStatus.py --status complete --flowcell %s --flowlane 0 --library %s --facility EDG' % (flowcell, userid)
            call_subprocess(cmd, shell=True, path=self.destination)
                    
    def ed_get_file(self, fname, project):
        '''Initiates download for fname and fname.md5 for the project. Returns fname has been downloaded and matches the md5.'''

        # NB! Note that the code below is agnostic to whether dowloading of the file or md5 file may have failed.
        
        ret = None
        
        # download file
        fn = ed_download_file(project + '/' + fname)
        # download md5 for the file
        fnmd5 = ed_download_file(project + '/' + fname + '.md5')
        # compute md5 for the downloaded file
        md5 = compute_md5(fn)
        with open(fnmd5, 'rb') as fh:
            for line in fh:
                line = line.rstrip('\n')
                if line != md5:
                    ret = None
                else:
                    ret = fn
                    break
        # Mark file on disk as downloaded successfully
        if ret is not None:
            cmd = 'touch %s.done' % fname
            call_subprocess(cmd, shell=True, path=self.destination)            
        return ret

    def ed_download_file(self, fname):
        '''Downloads file with name fname'''

        for f in data_files:
            acmd = "ascp -q -T -p -P %s -O %s -l %s %s@%s:%s %s" % (self.aPport, self.aOport, self.arate, self.credentials['username'], self.ahost, fname, self.destination)        
            call_subprocess(acmd, shell=True, path=self.destination) # Make sure ASPERA_SCP_PASS is available in shell. 
            fpath = os.path.join(self.destination, fname)
        return fpath


##################  M A I N   P R O G R A M  ######################

if __name__ == '__main__':

    from argparse import ArgumentParser
    
    PARSER = ArgumentParser(description='Submission of project related files to ENA')
    PARSER.add_argument('-p', '--project', dest='project', type=str, help='Project name in Edinburgh.', required=True)
    GROUP = parser.add_mutually_exclusive_group()
    GROUP.add_argument('-i', '--input', dest='fin', type=str, help='Input file containing library identifiers (donumbers) one per line. \'latest_plate_donumbers.txt\'')
    GROUP.add_argument('-l', '--library', dest='library', type=str, help='Library identifier (donumber, from Edinburgh systems point of view userid). E.g. \'do9555\'')
    GROUP.add_argument('-f', '--file', dest='fname', type=str, help='File name in Edinburgh system. E.g. \'do9555_R1.fastq.gz\'')
    
    # PARSER.add_argument('-s', '--split', dest='split', help='Files in the input have been demultiplexed per lane.', action='store_true')

    ARGS = PARSER.parse_args()
    edd = ed_downloader()

    # Process by input file containing donumbers/userids
    if ARGS.fin:
        edd.ed_download(ifile=ARGS.fin, project=ARGS.project)
    # Process by donumber/userdi
    elif ARGS.library:        
        edd.ed_get_files_by_userid(userid=ARGS.library, project=ARGS.project)
    # Process by file
    else:        
        edd.ed_get_file(fname=ARGS.fname, project=ARGS.project)
