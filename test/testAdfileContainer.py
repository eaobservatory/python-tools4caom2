#!/usr/bin/env python2.7

from __future__ import absolute_import

__author__ = "Russell O. Redman"
__version__ = "1.0"

import filecmp
import numpy
import os
import os.path
import re
import string
import sys
import tempfile
import unittest

from tools4caom2.caom2ingest import make_file_id
from tools4caom2.caom2ingest import fitsfilter
from tools4caom2.basecontainer import basecontainer
from tools4caom2.adfile_container import adfile_container
from tools4caom2.data_web_client import data_web_client
from tools4caom2.error import CAOMError

from .write_fits import write_fits


class testAdfileContainer(unittest.TestCase):
    """
    unit tests for tools4caom2.adfile_container class
    """
    def setUp(self):
        """
        Create a set of fits files (file6, file7, file8) and store them
        in an adfile container.  Include a non-FITS file file9.txt to verify
        that filtering works as intended.
        """
        # save the argument vector
        self.argv = sys.argv

        # set up the test envirnonment
        self.testdir = tempfile.mkdtemp()

        self.dataweb = data_web_client(self.testdir)

        # fake data
        fakedata = numpy.arange(10)

        # Create fits files with suitable test headers
        # Files file[1-4].fits will be present in the working directory
        # File file5.fits will be in file5.tar.gz.
        # Files file[6-8].fits will be in file6.ad.
        # There are also some garbage files to be ignored.

        write_fits(os.path.join(self.testdir, 'file6.fits'),
                   numexts=0,
                   obsid='obs4',
                   product='A')

        write_fits(os.path.join(self.testdir, 'file7.fits'),
                   numexts=0,
                   obsid='obs4',
                   product='B',
                   provenance='file6')

        write_fits(os.path.join(self.testdir, 'file8.fits'),
                   numexts=2,
                   obsid='obs4',
                   product='C',
                   provenance='file7')

        TEXT = open(os.path.join(self.testdir, 'file9.txt'), 'w')
        print >>TEXT, "This is some text"
        TEXT.close()

        # Create an ad file containing file6.fits, file7.fits and file8.fits
        # and file9.txt, i.e. no filtering in the AD file itself.
        # Save these files in the subdirectory 'save'
        self.savedir = os.path.join(self.testdir, 'save')
        os.mkdir(self.savedir)
        ADFILE = open(os.path.join(self.testdir, 'file6.ad'), 'w')
        for f in ['file6.fits', 'file7.fits', 'file8.fits', 'file9.txt']:
            filepath = os.path.join(self.testdir, f)
            file_id = make_file_id(f)

            ok = self.dataweb.put(filepath, 'TEST', file_id, adstream='test')
            if not ok:
                raise RuntimeError(cmd + ': ' + output)

            os.rename(filepath, os.path.join(self.savedir, f))

            print >>ADFILE, 'ad:TEST/' + file_id
        ADFILE.close()

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

    def test010_adfile_container_no_filter(self):
        """
        Test adfile_container implementations with no filtering
        """
        test_list = ['file6', 'file7', 'file8', 'file9']
        adfilepath = os.path.join(self.testdir, 'file6.ad')

        # Verify that the files are in ad
        for fid in test_list:
            headers = self.dataweb.info('TEST', fid)
            if not headers:
                raise CAOMError('ERROR: file not in ad: ' + fid +
                                ': ' + output)

        # Make a subdirectory in testdir to hold the files from ad for
        # comparison with the originals
        workdir = os.path.join(self.testdir, 'work')
        os.mkdir(workdir)
        test_file_list = [os.path.join(workdir, f) for f in
                          ['file6.fits',
                           'file7.fits',
                           'file8.fits',
                           'file9.txt']]

        # These files should all exist in ad so creation of an
        #  adfile_container should succeed.
        # The filetrfunc in this test is None.
        fc = adfile_container(adfilepath,
                              workdir,
                              None)

        # Verify that the list of file_id's returned by fc1.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.  File9 should be present in this list,
        # even though it is not a FITS file.
        self.assertEquals(set(test_list), set(fc.file_id_list()),
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(test_list)),
                           repr(sorted(fc.file_id_list()))))

        for i, file_id in enumerate(test_list):
            filepath = fc.get(file_id)
            # verify that the file path returned by get is identical to the
            # path that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # verify that the file is identical to its original
            self.assertTrue(filecmp.cmp(os.path.join(self.savedir,
                                                     test_file_list[i]),
                                        filepath))

            # Verify that cleanup removes the file
            fc.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

    def test020_adfile_container(self):
        """
        Test adfile_container implementations with FITS filtering
        """
        test_list = ['file6', 'file7', 'file8']
        adfilepath = os.path.join(self.testdir, 'file6.ad')

        # Verify that the files are in ad
        for fid in test_list:
            headers = self.dataweb.info('TEST', fid)
            if not headers:
                raise CAOMError('ERROR: file not in ad: ' + fid +
                                ': ' + output)

        # Make a subdirectory in testdir to hold the files from ad for
        # comparison with the originals
        workdir = os.path.join(self.testdir, 'work')
        os.mkdir(workdir)
        test_file_list = [os.path.join(workdir, f) for f in
                          ['file6.fits',
                           'file7.fits',
                           'file8.fits']]

        # These files should all exist in ad so creation of an
        #  adfile_container should succeed.
        # the filterfunc in this test is tools4caom2.caom2ingest.fitsfilter
        fc = adfile_container(adfilepath,
                              workdir,
                              fitsfilter)

        # Verify that the list of file_id's returned by fc1.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.  File9 should be present in this list,
        # even though it is not a FITS file.
        self.assertEquals(set(test_list), set(fc.file_id_list()),
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(test_list)),
                           repr(sorted(fc.file_id_list()))))

        for i, file_id in enumerate(test_list):
            filepath = fc.get(file_id)
            # verify that the file path returned by get is identical to the
            # path that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # verify that the file is identical to its original
            self.assertTrue(filecmp.cmp(os.path.join(self.savedir,
                                                     test_file_list[i]),
                                        filepath))

            # Verify that cleanup removes the file
            fc.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

    def test020_adfile_container(self):
        """
        Test adfile_container implementations with FITS filtering
        """
        test_list = ['file6', 'file7', 'file8']
        adfilepath = os.path.join(self.testdir, 'file6.ad')

        # Verify that the files are in ad
        for fid in test_list:
            headers = self.dataweb.info('TEST', fid)
            if headers:
                raise CAOMError('ERROR: file not in ad: ' + fid +
                                ': ' + output)

        # Make a subdirectory in testdir to hold the files from ad for
        # comparison with the originals
        workdir = os.path.join(self.testdir, 'work')
        os.mkdir(workdir)
        test_file_list = [os.path.join(workdir, f) for f in
                          ['file6.fits',
                           'file7.fits',
                           'file8.fits']]

        # These files should all exist in ad so creation of an
        #  adfile_container should succeed.
        # the filterfunc in this test is tools4caom2.caom2ingest.fitsfilter
        fc = adfile_container(adfilepath,
                              workdir,
                              fitsfilter)

        # run the test with use
        for i, file_id in enumerate(test_list):
            with fc.use(file_id) as filepath:
                # verify that the file path returned by use is identical to
                # the path that was inserted
                self.assertEquals(filepath, test_file_list[i],
                                  'ERROR: %s != %s' %
                                  (filepath, test_file_list[i]))

                # Verify that the file exists
                self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            self.assertTrue(not os.path.exists(filepath))

        # If we ask for an adfile name with a different extension,
        # the init should throw a LoggerError
        self.assertRaises(CAOMError,
                          adfile_container,
                          'bogus.file',
                          self.testdir,
                          None)

        # If the output directory does not exist, the init should throw a
        # LoggerError
        self.assertRaises(CAOMError,
                          adfile_container,
                          adfilepath,
                          '/junk/bogus',
                          None)

        # If we request a bogus file_id, this should raise a
        # LoggerError
        self.assertRaises(CAOMError,
                          fc.get, 'bogus')
