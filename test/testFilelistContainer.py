#!/usr/bin/env python2.7

from __future__ import absolute_import

__author__ = "Russell O. Redman"
__version__ = "1.0"

import logging
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
from tools4caom2.error import CAOMError
from tools4caom2.filelist_container import filelist_container

from .write_fits import write_fits


class testFilelistContainer(unittest.TestCase):
    """
    unit tests for tools4caom2.file-container classe
    """
    def setUp(self):
        """
        Create a set of fits files (file1, ... , file8).
        """
        # save the argument vector
        self.argv = sys.argv

        # set up the test envirnonment
        self.testdir = tempfile.mkdtemp()
        # fake data
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
                   provenance='file1')

        write_fits(os.path.join(self.testdir, 'file3.fits'),
                   numexts=0,
                   obsid='obs2',
                   product='A')

        # Add a non-FITS file to verify filtering
        TEXT = open(os.path.join(self.testdir, 'file9.txt'), 'w')
        print >>TEXT, "This is some text"
        TEXT.close()

    def tearDown(self):
        """
        Delete the testdir and any files it contains.
        Use walk in case we need to decend into subdirectories in the future.
        """
        for (dirpath, dirnames, filenames) in os.walk(self.testdir,
                                                      topdown=False):
            for filename in filenames:
                os.remove(os.path.join(dirpath, filename))
        os.rmdir(self.testdir)

        # Restore the system argument vector
        sys.argv = self.argv

    # Test filelist_container implementations
    def test010_filelist_container_all_fits(self):
        test_file_id_list = ['file1', 'file2', 'file3']
        test_file_list = [os.path.join(self.testdir, f + '.fits')
                          for f in test_file_id_list]

        # These files should all exist so creation of a filelist_container
        # should succeed
        fc = filelist_container('filelist',
                                test_file_list,
                                fitsfilter,
                                make_file_id)

        # Verify that the list of file_id's returned by fc1.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.
        self.assertEquals(set(test_file_id_list), set(fc.file_id_list()),
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(test_file_id_list)),
                           repr(sorted(fc.file_id_list()))))

        for i, file_id in enumerate(test_file_id_list):
            filepath = fc.get(file_id)
            # verify that the file path returned by get is identical to the
            # path that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup does nothing
            fc.cleanup(file_id)
            self.assertTrue(os.path.exists(filepath))

        # repeat the test with use
        for i, file_id in enumerate(test_file_id_list):
            with fc.use(file_id) as filepath:
                # verify that the file path returned by use is identical to
                # the path that was inserted
                self.assertEquals(filepath, test_file_list[i],
                                  'ERROR: %s != %s' %
                                  (filepath, test_file_list[i]))

                # Verify that the file exists
                self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup does nothing
            self.assertTrue(os.path.exists(filepath))

        # If we request a bogus file_id, this should raise a logger error
        self.assertRaises(KeyError,
                          fc.get, 'bogus')

        # close should do nothing
        fc.close()

        # If we append a bogus file name, the init should throw an IOError
        test_file_list.append('bogus.file')
        self.assertRaises(CAOMError,
                          filelist_container,
                          'filelist',
                          test_file_list,
                          None,
                          make_file_id)

    # Test filelist_container implementations
    def test020_filelist_container_no_filtering(self):
        test_file_id_list = ['file1', 'file2', 'file3', 'file9']
        test_file_list = [os.path.join(self.testdir, f) for f in
                          ['file1.fits',
                           'file2.fits',
                           'file3.fits',
                           'file9.txt']]

        # These files should all exist so creation of a filelist_container
        # should succeed.
        # This container should include file9.txt, since the filterfunc is None
        fc = filelist_container('filelist',
                                test_file_list,
                                None,
                                make_file_id)

        # Verify that the list of file_id's returned by fc1.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.
        self.assertEquals(set(test_file_id_list), set(fc.file_id_list()),
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(test_file_id_list)),
                           repr(sorted(fc.file_id_list()))))

        for i, file_id in enumerate(test_file_id_list):
            filepath = fc.get(file_id)
            # verify that the file path returned by get is identical to the
            # path that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup does nothing
            fc.cleanup(file_id)
            self.assertTrue(os.path.exists(filepath))

        # repeat the test with use
        for i, file_id in enumerate(test_file_id_list):
            with fc.use(file_id) as filepath:
                # verify that the file path returned by use is identical to
                # the path that was inserted
                self.assertEquals(filepath, test_file_list[i],
                                  'ERROR: %s != %s' %
                                  (filepath, test_file_list[i]))

                # Verify that the file exists
                self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup does nothing
            self.assertTrue(os.path.exists(filepath))

    # Test filelist_container implementations
    def test030_filelist_container_with_filtering(self):
        test_file_id_list = ['file1', 'file2', 'file3']
        test_file_list = [os.path.join(self.testdir, f) for f in
                          ['file1.fits',
                           'file2.fits',
                           'file3.fits',
                           'file9.txt']]

        # These files should all exist so creation of a filelist_container
        # should succeed.
        # This container should include file9.txt, since the filterfunc is None
        fc = filelist_container('filelist',
                                test_file_list,
                                fitsfilter,
                                make_file_id)

        # Verify that the list of file_id's returned by fc1.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.
        self.assertEquals(set(test_file_id_list), set(fc.file_id_list()),
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(test_file_id_list)),
                           repr(sorted(fc.file_id_list()))))

        self.assertEqual(fc.name, 'filelist')

        for i, file_id in enumerate(test_file_id_list):
            filepath = fc.get(file_id)
            # verify that the file path returned by get is identical to the
            # path that was inserted for each fits file
            if file_id != 'fits9':
                self.assertEquals(filepath, test_file_list[i],
                                  'ERROR: %s != %s' %
                                  (filepath, test_file_list[i]))

                # Verify that the file exists
                self.assertTrue(os.path.exists(filepath))

                # Verify that cleanup does nothing
                fc.cleanup(file_id)
                self.assertTrue(os.path.exists(filepath))

            else:
                # file9 is text and should have been filtered out
                self.assertTrue(file_id not in fc.file_id_list(),
                                'file9 is a text file and should have been '
                                'filtered out of the container, but is in ' +
                                repr(fc.file_id_list()))
