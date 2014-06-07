#!/usr/bin/env python

__author__ = "Russell O. Redman"

import httplib
import logging
import os.path
import re
import requests
import sys

from tools4caom2.logger import logger

from tools4caom2 import __version__

"""
Python implementation of the CADC data web service documented at
http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/.
"""

class data_web_service(object):
    
    PrimaryHEADER = {'fhead': 'true', 'cutout': '[0]'}
    
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
        self.service = 'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub'
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
        url = '/'.join([self.service, archive, file_id])
        r = requests.get(url, cert=self.cadcproxy)
        if r.status_code != 200:
            self.log.console(str(r.status_code) + ' = ' + 
                             httplib.responses[r.status_code],
                             logging.ERROR)
        # copy dictionary for usage after r is closed
        headerdict = {}
        headerdict.update(r.headers)
        self.log.file('headerdict for ' + url + ' = ' + repr(headerdict),
                      logging.DEBUG)
            
        return headerdict

    def get(self, archive, file_id, params={}, noclobber=False):
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
          data_web_service.PrimaryHEADER
        The resulting partial FITS file can be read with
          pyfits.getheader(filepath, 0)
        Beware that cutouts modify the file name, so use the filepath returned
        from get().
        
        See http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/
        for more explanation and examples.
        
        Arguments:
        archive: The archive from which the file should be fetched
        file_id: lower-case basename of the file
        params: (optional) dictionary of URL parameters
        noclobber: If true and the requested file exists, do not overwrite
        
        Returns:
        filepath: the file path of the new file on disk
        """
        url = '/'.join([self.service, archive, file_id])
        filepath = ''
        gzipped = False

        r = requests.get(url,
                         params=params,
                         cert=self.cadcproxy,
                         stream=True)
        if r.status_code != 200:
            self.log.console(str(r.status_code) + ' = ' + 
                             httplib.responses[r.status_code],
                             logging.ERROR)
        
        # get the original filename
        filename = file_id
        if 'content-disposition' in r.headers:
            m = re.match(r'^.*?filename=(.+)$', 
                         r.headers['content-disposition'])
            if m:
                filename = m.group(1)
        
        # restore the unzipped name if compressed
        # gzipped = (r.encoding.find('gzipped') >= 0)
        
        # can the encoding and the file name be incompatible???
        # possible source of trouble
        # if gzipped:
        unzipped, ext = os.path.splitext(filename)
        if ext == '.gz':
            filename = unzipped
            gzipped = True
        elif ext == '.ftz':
            filename = unzipped + '.fits'
            gzipped = True

        filepath = os.path.join(self.workdir, filename)
        if noclobber and os.path.exists(filepath):
            self.log.console('No download because file exists: ' + 
                             url,
                             logging.WARN)
        else:
            with open(filepath, 'wb') as F:
                for chunk in r.iter_content(8192):
                    F.write(chunk)
        
        return filepath

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
        myfilepath = os.path.abspath(
                        os.path.expanduser(
                            os.path.expandvars(filepath)))
        if not os.path.isfile(myfilepath):
            self.log.console('File Not Found ' + myfilepath,
                             logging.ERROR)
            
        url = '/'.join([self.service, archive, file_id])
        headers = {}
        if adstream:
            headers['X-CADC-Stream'] = adstream

        with open(myfilepath, 'rb') as F:
            r = requests.put(url,
                             data=F,
                             cert=self.cadcproxy,
                             headers=headers)
            if r.status_code != 201:
                self.log.console(str(r.status_code) + ' = ' + 
                                 httplib.responses[r.status_code],
                                 logging.ERROR)

    def delete(self, archive, file_id):
        """
        If the caller is authorized to access the archive, determine if the
        file_id exists and if so return basic HTTP header information.
        """
        headerdict = {}
        url = '/'.join([self.service, archive, file_id])
        r = requests.delete(url, 
                            cert=self.cadcproxy)
        if r.status_code != 200:
            self.log.console(str(r.status_code) + ' = ' + 
                             httplib.responses[r.status_code],
                             logging.ERROR)

if __name__ == '__main__':
    import os.path
    from tools4caom2.data_web_service import data_web_service
    from tools4caom2.logger import logger
    logpath = os.path.expanduser('~/junk/dws.log')
    log = logger(logpath)
    dws = data_web_service('~/junk', log)
    f = dws.get('JCMT', sys.argv[1])
    print f

