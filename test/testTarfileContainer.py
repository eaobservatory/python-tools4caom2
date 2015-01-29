#!/usr/bin/env python2.7

from __future__ import absolute_import

__author__ = "Russell O. Redman"
__version__ = "1.0"

import commands
import numpy
import os
import os.path
import re
import string
import sys
import tarfile
import tempfile
import unittest

from tools4caom2.caom2ingest import make_file_id
from tools4caom2.caom2ingest import fitsfilter
from tools4caom2.basecontainer import basecontainer
from tools4caom2.error import CAOMError
from tools4caom2.tarfile_container import tarfile_container

from .write_fits import write_fits


class testTarfileContainer(unittest.TestCase):
    """
    unit tests for tools4caom2.TarfileContainer classe
    """
    def setUp(self):
        """
        Create a set of fits files (file1, ... , file5) and store them in
        a tarfile.
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

        write_fits(os.path.join(self.testdir, 'file4.fits'),
                   numexts=0,
                   obsid='obs2',
                   product='B',
                   provenance='file3')

        write_fits(os.path.join(self.testdir, 'file5.fits'),
                   numexts=2,
                   obsid='obs3',
                   product='C',
                   member=['file1', 'file3'],
                   provenance=['file2', 'file4'])

        # Add a non-FITS file to verify filtering
        TEXT = open(os.path.join(self.testdir, 'file9.txt'), 'w')
        print >>TEXT, "This is some text"
        TEXT.close()

        # Create a tarball containing the fits files
        tarfilelist = ['file1.fits',
                       'file2.fits',
                       'file3.fits',
                       'file4.fits',
                       'file5.fits',
                       'file9.txt']
        tarfilestring = ' '.join(tarfilelist)

        cmd = ('cd ' + self.testdir +
               '; tar cvzf file5.tar.gz ' + tarfilestring +
               '; rm ' + tarfilestring)
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise exceptions.RuntimeError('could not create file5.tar.gz: '
                                          + output)

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

    # Test tarfile_container implementations
    def test010_tarfile_container_no_filtering(self):
        test_file_id_list = ['file1',
                             'file2',
                             'file3',
                             'file4',
                             'file5',
                             'file9']
        test_file_list = [os.path.join(self.testdir, f) for f in
                          ['file1.fits',
                           'file2.fits',
                           'file3.fits',
                           'file4.fits',
                           'file5.fits',
                           'file9.txt']]
        # The gzipped tar file exists so creation of a tarfile_container
        # should succeed.
        # The filterfunc is None.
        tarfilepath = os.path.join(self.testdir, 'file5.tar.gz')
        fc = tarfile_container(tarfilepath,
                               self.testdir,
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
            # verify that the filepath returned by get is identical to the path
            # that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            fc.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

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

            # Verify that cleanup removes the file
            self.assertTrue(not os.path.exists(filepath))

        # If we request a bogus file_id, this should raise a logger error
        self.assertRaises(CAOMError,
                          fc.get, 'bogus')

        fc.close()

        # If we ask for a bogus tar file name, the init should throw an IOError
        self.assertRaises(IOError,
                          tarfile_container,
                          'bogus.file',
                          self.testdir,
                          True,
                          make_file_id)

        # If the output directory does not exist, the init should throw a
        # LoggerError
        self.assertRaises(CAOMError,
                          tarfile_container,
                          tarfilepath,
                          '/junk/bogus',
                          True,
                          make_file_id)

    # Test tarfile_container implementations
    def test020_tarfile_container_no_filtering(self):
        test_file_id_list = ['file1',
                             'file2',
                             'file3',
                             'file4',
                             'file5']
        test_file_list = [os.path.join(self.testdir, f) for f in
                          ['file1.fits',
                           'file2.fits',
                           'file3.fits',
                           'file4.fits',
                           'file5.fits']]
        # The gzipped tar file exists so creation of a tarfile_container
        # should succeed.
        # The filterfunc is fitsfilter.
        tarfilepath = os.path.join(self.testdir, 'file5.tar.gz')
        fc = tarfile_container(tarfilepath,
                               self.testdir,
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
            # verify that the filepath returned by get is identical to the path
            # that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            fc.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

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

            # Verify that cleanup removes the file
            self.assertTrue(not os.path.exists(filepath))
