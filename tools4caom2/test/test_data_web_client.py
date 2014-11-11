#!/usr/bin/env python2.7
__author__ = "Russell O. Redman"

from datetime import datetime
import logging
import numpy as np
import os
import os.path
import pyfits
import re
import tempfile
import unittest

from tools4caom2.logger import logger

from tools4caom2.data_web_client import data_web_client

class testDataWebService(unittest.TestCase):
    def setUp(self):
        """
        Create a temp directory and data_web_client object
        """
        self.tmpdir = tempfile.mkdtemp()
        self.logfile = 'web_data_service.log'
        self.log = logger(self.logfile, 
                          loglevel=logging.DEBUG, 
                          console_output=False)
        self.web_service = data_web_client(self.tmpdir,
                                           self.log)
        
    def tearDown(self):
        """
        delete the temporary file and directory
        """
        for f in os.listdir(self.tmpdir):
            os.remove(os.path.join(self.tmpdir, f))
        os.rmdir(self.tmpdir)
        
    def test_not_found(self):
        """
        get info on a nonexistent file
        """
        headers = self.web_service.info('JCMT', 'bogusfile')
        self.assertEqual(headers, {})

    def test_info_found(self):
        """
        get info on a real file, acsis_00002_20140101T080531
        """
        http_header = self.web_service.info('JCMT', 'a20140101_00002_01_0001')
        self.assertEqual(http_header['content-disposition'],
                         'inline; filename=a20140101_00002_01_0001.sdf.gz')

    def test_get(self):
        """
        get gzipped sdf and ungzipped fits files.
        """
        fid1 = 'a20140101_00002_01_0001'
        fp1 = self.web_service.get('JCMT', fid1)
        self.assertEqual(fp1, os.path.join(self.tmpdir, 
                                           'a20140101_00002_01_0001.sdf'))
        fid2 = 'jcmts20140314_00009_850_reduced001_nit_000'
        fp2 = self.web_service.get('JCMT', fid2)
        self.assertEqual(fp2, os.path.join(
                         self.tmpdir,
                         'jcmts20140314_00009_850_reduced001_nit_000.fits'))
        
    def test_put(self):
        """
        Create FITS file put into TEST archive, then verify that it can be found
        """
        filename = 'data_web_client_test.fits'
        file_id, ext = os.path.splitext(filename)
        filepath = os.path.join(self.tmpdir, filename)
        
        # Create a test with unique metadata
        data = np.arange(100)
        hdu = pyfits.PrimaryHDU(data)
        hdulist = pyfits.HDUList([hdu])
        hdulist.writeto(filepath)
        
        s = self.web_service.put(filepath, 'TEST', file_id, adstream='test')
        with open(self.logfile,'r') as L:
            logmsg = L.read()
        self.assertTrue(s, 'failed to put ' + filepath + ' to TEST with ' +
                        ' file_id = ' + file_id + ': logmsg = ' + logmsg)
            
        d = self.web_service.info('TEST', file_id)
        self.assertTrue(d['content-disposition'].find(file_id) > 0)
        
    def test_cutout(self):
        """
        Create FITS file put into TEST archive, then get the head and verify
        that it contains the same DATE header
        """
        filename = 'data_web_client_test.fits'
        file_id, ext = os.path.splitext(filename)
        filepath = os.path.join(self.tmpdir, filename)
        
        mydate = datetime.now().isoformat()
        
        # Create a test with unique metadata
        data = np.arange(100)
        hdu = pyfits.PrimaryHDU(data)
        hdu.header['DATE'] = mydate
        hdulist = pyfits.HDUList([hdu])
        hdulist.writeto(filepath)
        
        s = self.web_service.put(filepath, 'TEST', file_id, adstream='test')
        logmsg = open(self.logfile,'r').read()
        self.assertTrue(s, 'failed to put ' + filepath + ' to TEST with ' +
                        ' file_id = ' + file_id + ': logmsg = ' + logmsg)
            
        d = self.web_service.get('TEST', 
                                 file_id,
                                 params=data_web_client.PrimaryHEADER)
        header_copy = pyfits.getheader(d, 0)
        self.assertTrue('DATE' in header_copy,
                        'The DATE header is missing from ' + d)
        self.assertEqual(mydate, header_copy['DATE'],
                         'The DATE header ' + header_copy['DATE'] + 
                         ' does not equal mydate=' + mydate) 
        
        

if __name__ == '__main__':
    unittest.main()
    
