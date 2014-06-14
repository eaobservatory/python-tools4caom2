#!/usr/bin/env python

__author__ = "Russell O. Redman"

import argparse
from ConfigParser import SafeConfigParser
import httplib
import logging
import os.path
import re
import requests
import sys
import traceback

from tools4caom2.logger import logger

from jcmt2caom2.jsa.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

"""
Python implementation of the CADC data web service documented at
http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/.
"""

class data_web_client(object):
    
    PrimaryHEADER = {'fhead': 'true', 'cutout': '[0]'}
    CADC_URL = 'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub'
    
    def __init__(self, 
                 workdir,
                 log, 
                 proxy='$HOME/.ssl/cadcproxy.pem'):
        """
        Access to data in the Archive Directory is supplied at the CADC through
        a data web service documented at:
          http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/
        
        Arguments:
        workdir: Directory to hold files fetched through the service
        log: an instance of a tools4caom2.logger
        proxy: (optional) path to a proxy certificate
        """
        self.log = log
        self.workdir = os.path.abspath(
                            os.path.expandvars(
                                os.path.expanduser(workdir)))
        if not os.path.isdir(self.workdir):
            self.log.console('workdir is not a directory: ' + self.workdir)
        self.cadcproxy = os.path.abspath(
                            os.path.expandvars(
                                os.path.expanduser(proxy)))

    def info(self, archive, file_id):
        """
        If the caller is authorized to access the archive, determine if the
        file_id exists and if so return basic HTTP header information.

        Arguments:
        archive: The archive from which the file should be fetched
        file_id: lower-case basename of the file
        """
        if file_id[:4] == 'http':
            url = re.sub(r'http:', 'https:', file_id)
        else:
            url = '/'.join([data_web_client.CADC_URL, archive, file_id])
        self.log.file('url = ' + url,
                      logging.DEBUG)
        
        headerdict = {}
        try:
            r = requests.get(url, cert=self.cadcproxy)
            if r.status_code != 200:
                self.log.console(str(r.status_code) + ' = ' + 
                                 httplib.responses[r.status_code],
                                 logging.WARN)
            else:
                # copy dictionary for usage after r is closed
                headerdict.update(r.headers)
        except Exception as e:
            self.log.console('FAILED to get info for ' + file_id + ': ' + 
                             traceback.format_exc(),
                             logging.WARN)
            
        return headerdict

    def get(self, 
            archive, 
            file_id, 
            filepath=None, 
            params={}, 
            noclobber=False):
        """
        Fetch a file from ad into the working directory.  
        
        URL parameters  are supplied through the dictionary params as key/value 
        pairs.  To fetch just the HDU headers pass  
          params={'fhead': 'true'}
        Similarly, for cutouts pass something like
          params={'cutout': '[1][1:100,1:200]'}
        For the common case where only the headers from the primary HDU are 
        wanted, pass
          params={'fhead': 'true', 'cutout': '[0]'}
        This is defined as the class constant:
          data_web_client.PrimaryHEADER
        The resulting partial FITS file can be read with
          pyfits.getheader(filepath, 0)
        Beware that cutouts modify the file name, so use the filepath returned
        from get().
        
        See http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/
        for more explanation and examples.
        
        Arguments:
        archive: The archive from which the file should be fetched
        file_id: lower-case basename of the file
        filepath: (optional) path where file will be stored
        params: (optional) dictionary of URL parameters
        noclobber: If true and the requested file exists, do not overwrite
        
        Returns:
        filepath: the file path of the new file on disk
        """
        myfilepath = None
        gzipped = False
        
        if file_id[:4] == 'http':
            url = re.sub(r'http:', 'https:', file_id)
        else:
            url = '/'.join([data_web_client.CADC_URL, archive, file_id])
        self.log.file('url = ' + url,
                      logging.DEBUG)
                
        try:
            r = requests.get(url,
                             params=params,
                             cert=self.cadcproxy,
                             stream=True)
            if r.status_code != 200:
                self.log.console(str(r.status_code) + ' = ' + 
                                 httplib.responses[r.status_code],
                                 logging.ERROR)
            
            # get the original filename
            myfilepath = os.path.join(self.workdir, file_id)
            if filepath:
                myfilepath = os.path.abspath(
                                os.path.expandvars(
                                    od.path.expanduser(filepath)))
                
            elif 'content-disposition' in r.headers:
                filename = file_id
                m = re.match(r'^.*?filename=(.+)$', 
                             r.headers['content-disposition'])
                if m:
                    filename = m.group(1)
            
                # restore the unzipped name if compressed
                # gzipped = (r.encoding.find('gzipped') >= 0)
                
                # can the encoding and the file name be incompatible???
                # possible source of trouble
                unzipped, ext = os.path.splitext(filename)
                if ext == '.gz':
                    filename = unzipped
                    gzipped = True
                elif ext == '.ftz':
                    filename = unzipped + '.fits'
                    gzipped = True
                myfilepath = os.path.join(self.workdir, filename)
                
            if noclobber and os.path.exists(myfilepath):
                self.log.console('No download because file exists: ' + 
                                 url,
                                 logging.WARN)
            else:
                with open(myfilepath, 'wb') as F:
                    for chunk in r.iter_content(8192):
                        F.write(chunk)
                # This message must match the format of the regex
                # in run(), below
                self.log.file('SUCCESS: got ' + file_id + ' as ' + 
                              myfilepath)
        except Exception as e:
            self.log.console('FAILED to get ' + file_id + ': ' + 
                             traceback.format_exc(),
                             logging.WARN)
        
        return myfilepath

    def put(self, filepath, archive, file_id, adstream=None):
        """
        If the caller is authorized to write to the archive, put the file
        from filepath into the requested archive and stream with the
        specified file_id.
        
        Arguments:
        filepath: path the the local file on disk
        archive: The archive from which the file should be fetched
        file_id: lower-case basename of the file
        adstream: (optional) archive stream
        """
        # verify that the file exists
        success = False
        myfilepath = os.path.abspath(
                        os.path.expanduser(
                            os.path.expandvars(filepath)))
        if not os.path.isfile(myfilepath):
            self.log.console('File Not Found ' + myfilepath,
                             logging.WARN)
            return success

        if file_id[:4] == 'http':
            url = re.sub(r'http:', 'https:', file_id)
        else:
            url = '/'.join([data_web_client.CADC_URL, archive, file_id])
        
        headers = {}
        if adstream:
            headers['X-CADC-Stream'] = adstream

        with open(myfilepath, 'rb') as F:
            r = requests.put(url,
                             data=F,
                             cert=self.cadcproxy,
                             headers=headers)
            if r.status_code == 201:
                success = True
            else:
                self.log.console(str(r.status_code) + ' = ' + 
                                 httplib.responses[r.status_code],
                                 logging.ERROR)

        return success

    def delete(self, archive, file_id):
        """
        If the caller is authorized to access the archive, determine if the
        file_id exists and if so return basic HTTP header information.
        """
        success = False
        headerdict = {}
        if file_id[:4] == 'http':
            url = re.sub(r'http:', 'https:', file_id)
        else:
            url = '/'.join([data_web_client.CADC_URL, archive, file_id])
        self.log.file('url = ' + url,
                      logging.DEBUG)
        
        try:
            r = requests.delete(url, cert=self.cadcproxy)
            if r.status_code == 200:
                success = True
            else:
                self.log.console(str(r.status_code) + ' = ' + 
                                 httplib.responses[r.status_code],
                                 logging.WARN)
        except Exception as e:
            self.log.console('FAILED to put ' + filepath + ': ' + 
                             traceback.format_exc(),
                             logging.WARN)

        return success

def run():
    """
    Provides a command line interface to file operations, to be used in
    the cadcdata command.  If none of --get, --put or --delete is
    specified, the default operation is 'info'.  Multiple fileid's 
    can be requested only for info and get operations.  Put and delete 
    operations require that the CADC authorize the user's account for 
    those operations on the requested archive.
    """
    utdate_str = utdate_string()
    
    ap = argparse.ArgumentParser('cadcdata',
                                 fromfile_prefix_chars='@')
    ap.add_argument('--proxy',
                    default='$HOME/.ssl/cadcproxy.pem',
                    help='path to CADC proxy')
    
    ap.add_argument('--log',
                    default='cadcdata_' + utdate_str + '.log',
                    help='(optional) name of log file')
    ap.add_argument('--debug',
                    action='store_true',
                    help='run ingestion commands in debug mode')
    ap.add_argument('--retry',
                    nargs='*',
                    help='log file(s) to parse for transfers to retry')

    ap.add_argument('-g', '--get',
                    dest='operation',
                    action='store_const',
                    const='get',
                    default='info',
                    help='get files from CADC (requires archive and '
                         'fileid, optionally file and noclobber)')
    ap.add_argument('-p', '--put',
                    dest='operation',
                    action='store_const',
                    const='put',
                    help='get files from CADC (requires archive, '
                         'file, fileid and CADC authorization, '
                         'optionally stream)')
    ap.add_argument('-d', '--delete',
                    dest='operation',
                    action='store_const',
                    const='delete',
                    help='delete a file from a CADC archive (requires '
                         'archive, fileid and CADC authorization)')

    ap.add_argument('-a', '--archive',
                    help='archive containing file')
    ap.add_argument('--stream',
                    help='stream for the archive during put operations')
    ap.add_argument('-f', '--file',
                    help='path to file for get and put operations')
    ap.add_argument('--noclobber',
                    action='store_true',
                    help='do not overwrite existing files')
    ap.add_argument('fileid',
                    nargs='+',
                    help='list of fileid\'s, url\'s or files containing them')
    
    a = ap.parse_args()

    # Open log and record switches
    cwd = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars('.')))
    
    cadcproxy = os.path.abspath(
                    os.path.expandvars(
                        os.path.expanduser(a.proxy)))
    
    loglevel = logging.INFO
    if a.debug:
        loglevel = logging.DEBUG
    
    if os.path.dirname(a.log):
        logpath = os.path.abspath(
                    os.path.expanduser(
                        os.path.expandvars(a.log)))
    else:
        logpath = os.path.join(cwd, a.log)
    
    with logger(logpath, loglevel).record() as log:
        log.file(sys.argv[0])
        log.file('tools4caom2version   = ' + tools4caom2version)
        log.console('log = ' + logpath)
        for attr in dir(a):
            if attr != 'id' and attr[0] != '_':
                log.console('%-15s= %s' % (attr, getattr(a, attr)),
                            logging.DEBUG)
        
        if a.operation == 'put' and not a.file:
            log.console('existing file must be supplied for put',
                        logging.ERROR)

        fileid_list = []
        fileid_list.extend(a.fileid)
        # Remove any fileid that have already been transfered
        if a.retry:
            for retrylog in a.retry:
                retry = os.path.abspath(
                           os.path.expandvars(
                                os.path.expanduser(retrylog)))
                
                if not os.path.isfile(retry):
                    log.console('retry log is not a file ' + 
                                     retry,
                                     logging.ERROR)
                with open(retry, 'r') as RETRY:
                    for line in RETRY:
                        # This must match the success message written 
                        # by get()
                        m = re.search(r'SUCCESS: got (\S+) ', line)
                        if m:
                            fileid = m.group(1)
                            if fileid in fileid_list:
                                fileid_list.remove(fileid)
                                log.file('remove ' + fileid)
                
        if a.file and len(a.fileid) != 1:
            log.console('if file is given, exactly one fileid must '
                             'be supplied',
                        logging.ERROR)
        
        filepath = None
        if a.file:
            filepath = os.path.abspath(
                           os.path.expanduser(
                               os.path.expandvars(a.file)))

        dwc = data_web_client(cwd, log, proxy=a.proxy)
        
        if a.operation == 'info':
            for fileid in fileid_list:
                headerdict = dwc.info(a.archive, fileid)
                if headerdict:
                    log.console('fileid = ' + fileid)
                    for key in headerdict:
                        log.console('    ' + key + ' = ' + 
                                         headerdict[key])
        
        elif a.operation == 'get':
            if filepath:
                dwc.get(a.archive, 
                        a.fileid[0], 
                        filepath=filepath, 
                        noclobber=a.noclobber)
            else:
                # Get all of the requested fileid
                for fileid in fileid_list:
                    dwc.get(a.archive, 
                            fileid, 
                            noclobber=a.noclobber)

        elif a.operation == 'put':
            dwc.put(filepath, a.archive, a.fileid[0], adstream=a.stream)
        
        elif a.operation == 'delete':
            dwc.delete(a.archive, a.fileid[0])
