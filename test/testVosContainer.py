#!/usr/bin/env python
# ***********************************************************************
#
#    Script Name:    testVosContainer.py
#
#    Purpose:
#     Unit test module for tools4caom2.vos_container
#
#    Classes:
#
#    Functions:
#
#    Modification History:
#
# ***  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
# ***********************************************************************

from __future__ import absolute_import

__author__ = "Russell O. Redman"
__version__ = "1.0"

from datetime import datetime
import filecmp
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

from tools4caom2.util import make_file_id_no_ext as make_file_id
from tools4caom2.container.vos import vos_container
from tools4caom2.data_web_client import data_web_client
from tools4caom2.validation import CAOMValidation
from tools4caom2.error import CAOMError

from .write_fits import write_fits


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

        self.dataweb = data_web_client(self.testdir)
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
        self.validation = CAOMValidation(self.testdir,
                                         'JCMT',
                                         fileid_regex_dict,
                                         make_file_id)
        # create the basic vos_container
        voscontainer = vos_container(self.vostest,
                                     'JCMT',  # for existing test data
                                     False,   # do not fetch files from
                                              # AD by default
                                     self.testdir,
                                     self.validation,
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
        self.assertRaises(CAOMError,
                          voscontainer.get,
                          'bogus')

    def test020_vos_container_use(self):
        """
        Test vos_container implementation using use
        """
        fileid_regex = re.compile(r'file.*')
        fileid_regex_dict = {'.fits': [fileid_regex]}
        self.validation = CAOMValidation(self.testdir,
                                         'JCMT',
                                         fileid_regex_dict,
                                         make_file_id)
        # create the basic vos_container
        voscontainer = vos_container(self.vostest,
                                     'JCMT',  # for existing test data
                                     False,  # do not fetch files from
                                             # AD by default
                                     self.testdir,
                                     self.validation,
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
        self.validation = CAOMValidation(self.testdir,
                                         'JCMT',
                                         fileid_regex_dict,
                                         make_file_id)
        # If we ask for an vos name that does not exist,
        # the init should throw a LoggerError
        self.assertRaises(CAOMError,
                          vos_container,
                          'vos:jsaops/bogus',
                          'JCMT',  # for existing test data
                          False,   # do not fetch files from
                                   # AD by default
                          self.testdir,
                          self.validation,
                          self.vosclient,
                          self.dataweb,
                          make_file_id)

        # If the output directory does not exist, the init should throw a
        # LoggerError
        bogusdir = os.path.join(self.testdir, 'bogus')
        self.assertRaises(CAOMError,
                          vos_container,
                          self.vostest,
                          'JCMT',  # for existing test data
                          False,   # do not fetch files from
                                   # AD by default
                          bogusdir,
                          self.validation,
                          self.vosclient,
                          self.dataweb,
                          make_file_id)

    def test040_vos_container_get_from_ad(self):
        """
        Test vos_container implementation using get from AD
        """
        fileid_regex = re.compile(r'jcmt.*')
        fileid_regex_dict = {'.fits': [fileid_regex]}
        self.validation = CAOMValidation(self.testdir,
                                         'JCMT',
                                         fileid_regex_dict,
                                         make_file_id)
        # create the basic vos_container
        voscontainer = vos_container(self.vosinad,
                                     'JCMT',  # for existing test data
                                     True,    # do not fetch files from
                                              # AD by default
                                     self.testdir,
                                     self.validation,
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
