#!/usr/bin/env python
#************************************************************************
#*
#*   Script Name:    testVosContainer.py
#*
#*   Purpose:
#+    Unit test module for tools4caom2.vos_container
#*
#*   Classes:
#*
#*   Functions:
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
__author__ = "Russell O. Redman"
__version__ = "1.0"


import commands
from datetime import datetime
import filecmp
import logging
import numpy
import os
import os.path
import pyfits
import re
import string
import sys
import tempfile
import unittest

import vos

from tools4caom2.ingest2caom2 import make_file_id
from tools4caom2.ingest2caom2 import fitsfilter
from tools4caom2.basecontainer import basecontainer
from tools4caom2.vos_container import vos_container
from tools4caom2.data_web_client import data_web_client
from tools4caom2.delayed_error_warning import delayed_error_warning
from tools4caom2.logger import logger


def write_fits(filepath,
               numexts,
               obsid,
               product,
               member=None,
               provenance=None):
    """
    Write a FITS test file with the requested PRODUCT keyword and number
    of extensions.

    Arguments:
    filepath  : path to the new file
    numexts   : number of extensions
    product   : product type

    In this example, inputs and provenance will be recorded using the file_id
    of the input file.
    """
    data = numpy.arange(10)
    datestring = datetime.utcnow().isoformat()
    hdu = pyfits.PrimaryHDU(data)
    # parse the filepath
    filebase = os.path.basename(filepath)
    file_id, ext = os.path.splitext(filebase)
    hdu.header.update('FILE-ID', file_id)
    hdu.header.update('COLLECT', 'TEST')
    hdu.header.update('OBSID', obsid)

    # DPDATE will be different every time the program runs, so it should be
    # possible to verify that the files have been updated in AD by checking
    # this header.
    hdu.header.update('DPDATE', datestring)
    hdu.header.update('PRODUCT', product)
    hdu.header.update('NUMEXTS', numexts)
    hdu.header.update('FIELD1', 'F1%s' % (product))
    hdu.header.update('FIELD2', 'F2%s' % (product))

    # Some product-dependent headers
    if product != 'A':
        hdu.header.update('FIELD3', 'F3%s' % (product))
        hdu.header.update('NOTA', True)
    else:
        hdu.header.update('NOTA', False)

    #Some extension-dependent headers
    hdu.header.update('FIELD4', 'BAD')
    hdu.header.update('FIELD5', 'GOOD')

    # Composite products have members identified by their file_id's
    if isinstance(member, list):
        hdu.header.update('OBSCNT', len(member))
        for i, name in enumerate(member):
            hdu.header.update('OBS%d' % (i + 1), name)
    elif isinstance(member, str):
        hdu.header.update('OBSCNT', '1')
        hdu.header.update('OBS1', member)

    # Derived products have inputs identified by their file_id's
    if isinstance(provenance, list):
        hdu.header.update('PRVCNT', len(provenance))
        for i, name in enumerate(provenance):
            hdu.header.update('PRV%d' % (i + 1), name)
    elif isinstance(provenance, str):
        hdu.header.update('PRVCNT', '1')
        hdu.header.update('PRV1', provenance)

    hdulist = pyfits.HDUList(hdu)

    # Optionally add extensions
    for extension in range(1, numexts + 1):
        hdu = pyfits.ImageHDU(data)
        hdu.header.update('EXTNAME', 'EXTENSION%d' % (extension))
        hdu.header.update('OBSID', obsid)
        hdu.header.update('PRODUCT', '%s%d' % (product, extension))
        hdu.header.update('DPDATE', datestring)
        hdu.header.update('FIELD1', 'F1%s%d' % (product, extension))
        hdu.header.update('FIELD2', 'F2%s%d' % (product, extension))

        # Product dependent headers
        if product != 'A':
            hdu.header.update('FIELD3', 'F3%s' % (product))
            hdu.header.update('NOTA', True)
        else:
            hdu.header.update('NOTA', False)

        # Extension-dependent headers
        hdu.header.update('FIELD4', 'GOOD')
        hdu.header.update('FIELD5', 'BAD')
        # an extension-specific header
        hdu.header.update('HEADER%d' % (extension),
                          'H%s%d' % (product, extension))

        hdulist.append(hdu)

    hdulist.writeto(filepath)

class testVosContainer(unittest.TestCase):
    """
    unit tests for tools4caom2.vos_container class
    """
    def setUp(self):
        """
        Create a set of fits files (file1, file2, file3) and store them 
        in an vos container.  Include a non-FITS file file4.txt to verify
        that filtering works as intended.
        """
        # save the argument vector
        self.argv = sys.argv

        # set up the test envirnonment
        self.testdir = tempfile.mkdtemp()
        self.log = logger(os.path.join(self.testdir, 'testvos.log'),
                           console_output=False)

        self.dataweb = data_web_client(self.testdir, self.log)
        self.vosclient = vos.Client()
        self.vostest = 'vos:jsaops/unittest/tempdata'
        self.vosinad = 'vos:jsaops/unittest/testdata'

        if not self.vosclient.isfile(self.vostest + '/file1.fits'):
            # make fake data
            fakedata = numpy.arange(10)

            # Create fits files with suitable test headers
            # Files file[1-4].fits will be present in the working directory
            # File file5.fits will be in file5.tar.gz.
            # Files file[6-8].fits will be in file6.ad.
            # There are also some garbage files to be ignored.

            write_fits(os.path.join(self.testdir, 'file1.fits'),
                       numexts=0,
                       obsid='obs1',
                       product='A')

            write_fits(os.path.join(self.testdir, 'file2.fits'),
                       numexts=0,
                       obsid='obs1',
                       product='B',
                       provenance='file6')

            write_fits(os.path.join(self.testdir, 'file3.fits'),
                       numexts=2,
                       obsid='obs1',
                       product='C',
                       provenance='file7')
            
            TEXT = open(os.path.join(self.testdir, 'file4.txt'), 'w')
            print >>TEXT, "This is some text"
            TEXT.close()
            
            # Push the files into vos:jsaops/unittest/testdata
            for f in ['file1.fits', 'file2.fits', 'file3.fits', 'file4.txt']:
                filepath = os.path.join(self.testdir, f)
                vospath = self.vostest + '/' + f
                if not self.vosclient.isfile(vospath):
                    size = self.vosclient.copy(filepath, vospath)
        
    def tearDown(self):
        """
        Delete the testdir and any files it contains.
        Use walk in case we need to decend into subdirectories in the future.
        """
        for (dirpath, dirnames, filenames) in os.walk(self.testdir,
                                                      topdown=False):
            for filename in filenames:
                os.remove(os.path.join(dirpath, filename))
            for dirname in dirnames:
                os.rmdir(os.path.join(dirpath, dirname))
        os.rmdir(self.testdir)

        # Restore the system argument vector
        sys.argv = self.argv

    def test010_vos_container_get(self):
        """
        Test vos_container implementation using get
        """
        fileid_regex = re.compile(r'file.*')
        fileid_regex_dict = {'.fits': [fileid_regex]}
        self.dew = delayed_error_warning(self.log,
                                         self.testdir,
                                         'JCMT',
                                         fileid_regex_dict,
                                         make_file_id)
        # create the basic vos_container
        voscontainer = vos_container(self.log,
                                     self.vostest,
                                     'JCMT', # for existing test data
                                     False,  # do not fetch files from 
                                             # AD by default
                                     self.testdir,
                                     self.dew,
                                     self.vosclient,
                                     self.dataweb,
                                     make_file_id)

        # Verify that the list of file_id's voscontainer.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.  File9 should be present in this list,
        # even though it is not a FITS file.
        testlist = ['file1', 'file2', 'file3']
        testset = set(testlist)
        vosset = set(voscontainer.file_id_list())
        self.assertEquals(testset, vosset,
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(list(testset))),
                           repr(sorted(list(vosset)))))

        for i, file_id in enumerate(testlist):
            filepath = voscontainer.get(file_id)
            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            voscontainer.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

        # If we request a bogus file_id, this should raise a 
        # LoggerError
        self.assertRaises(logger.LoggerError,
                          voscontainer.get, 
                          'bogus')
        
    def test020_vos_container_use(self):
        """
        Test vos_container implementation using use
        """
        fileid_regex = re.compile(r'file.*')
        fileid_regex_dict = {'.fits': [fileid_regex]}
        self.dew = delayed_error_warning(self.log,
                                         self.testdir,
                                         'JCMT',
                                         fileid_regex_dict,
                                         make_file_id)
        # create the basic vos_container
        voscontainer = vos_container(self.log,
                                     self.vostest,
                                     'JCMT', # for existing test data
                                     False,  # do not fetch files from 
                                             # AD by default
                                     self.testdir,
                                     self.dew,
                                     self.vosclient,
                                     self.dataweb,
                                     make_file_id)

        testlist = ['file1', 'file2', 'file3']
        testset = set(testlist)
        vosset = set(voscontainer.file_id_list())

        # run the test with use
        for i, file_id in enumerate(testlist):
            with voscontainer.use(file_id) as filepath:
                # Verify that the file exists
                self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            self.assertTrue(not os.path.exists(filepath))
        
    def test030_vos_container_errors(self):
        """
        Test vos_container implementation errors
        """
        fileid_regex = re.compile(r'file.*')
        fileid_regex_dict = {'.fits': [fileid_regex]}
        self.dew = delayed_error_warning(self.log,
                                         self.testdir,
                                         'JCMT',
                                         fileid_regex_dict,
                                         make_file_id)
        # If we ask for an vos name that does not exist, 
        # the init should throw a LoggerError
        self.assertRaises(logger.LoggerError,
                          vos_container,
                          self.log,
                          'vos:jsaops/bogus',
                          'JCMT', # for existing test data
                          False,  # do not fetch files from 
                                  # AD by default
                          self.testdir,
                          self.dew,
                          self.vosclient,
                          self.dataweb,
                          make_file_id)

        # If the output directory does not exist, the init should throw a 
        # LoggerError
        bogusdir = os.path.join(self.testdir, 'bogus')
        self.assertRaises(logger.LoggerError,
                          vos_container,
                          self.log,
                          self.vostest,
                          'JCMT', # for existing test data
                          False,  # do not fetch files from 
                                  # AD by default
                          bogusdir,
                          self.dew,
                          self.vosclient,
                          self.dataweb,
                          make_file_id)

    def test040_vos_container_get_from_ad(self):
        """
        Test vos_container implementation using get from AD
        """
        fileid_regex = re.compile(r'jcmt.*')
        fileid_regex_dict = {'.fits': [fileid_regex]}
        self.dew = delayed_error_warning(self.log,
                                         self.testdir,
                                         'JCMT',
                                         fileid_regex_dict,
                                         make_file_id)
        # create the basic vos_container
        voscontainer = vos_container(self.log,
                                     self.vosinad,
                                     'JCMT', # for existing test data
                                     True,  # do not fetch files from 
                                             # AD by default
                                     self.testdir,
                                     self.dew,
                                     self.vosclient,
                                     self.dataweb,
                                     make_file_id)

        # Verify that the list of file_id's voscontainer.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.  File9 should be present in this list,
        # even though it is not a FITS file.
        testlist = ['jcmts20140322_00044_450_reduced001_obs_000',
                    'jcmts20140322_00044_850_reduced001_obs_000']
        testset = set(testlist)
        vosset = set(voscontainer.file_id_list())
        self.assertEquals(testset, vosset,
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(list(testset))),
                           repr(sorted(list(vosset)))))

        for i, file_id in enumerate(testlist):
            filepath = voscontainer.get(file_id)
            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            voscontainer.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

if __name__ == '__main__':
    unittest.main()

