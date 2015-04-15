#!/usr/bin/env python2.7

from __future__ import absolute_import

__author__ = "Russell O. Redman"

import numpy
import os
import os.path
from astropy.io import fits
import re
import subprocess
import tempfile
import unittest

from tools4caom2.data_web_client import data_web_client
from tools4caom2.delayed_error_warning import delayed_error_warning

from .write_fits import write_fits


def make_file_id(filename):
    return os.path.basename(os.path.splitext(filename)[0]).lower()


class test_delayed_error_warning(unittest.TestCase):
    def setUp(self):
        """
        Create a temp directory and delayed_error_warning object.  The
        temporary directory must be shared amongst several subprocesses, so we
        cannot use mkdtemp.
        """
        self.tmpdir = tempfile.mktemp(dir='/tmp')
        os.mkdir(self.tmpdir)

        fileid_regex_dict = {'.fits': [re.compile(r'test_.*'),
                                       re.compile(r'archive_.*')],
                             '.png': [re.compile(r'TEST_.*')]}

        self.dew = delayed_error_warning(self.tmpdir,
                                         'JCMT',
                                         fileid_regex_dict,
                                         make_file_id)

        # fake data
        fakedata = numpy.arange(10)

        # Create fits files with suitable test headers
        # Files file[1-4].fits will be present in the working directory
        # File file5.fits will be in file5.tar.gz.
        # Files file[6-8].fits will be in file6.ad.
        # There are also some garbage files to be ignored.
        self.test_file = os.path.join(self.tmpdir, 'test_file.fits')
        write_fits(self.test_file,
                   numexts=0,
                   obsid='obs1',
                   product='A')

        self.archive_file = os.path.join(self.tmpdir, 'archive_file.fits')
        write_fits(self.archive_file,
                   numexts=0,
                   obsid='obs1',
                   product='B')

        self.bogus_file = os.path.join(self.tmpdir, 'bogus_file.fits')
        write_fits(self.bogus_file,
                   numexts=0,
                   obsid='obs2',
                   product='A',
                   badheader=('EPOCH', 2000.0))  # deprecated header warning

        # Add a non-FITS file to verify filtering
        self.empty_file = os.path.join(self.tmpdir, 'empty_file.txt')
        TEXT = open(self.empty_file, 'w')
        TEXT.close()

    def tearDown(self):
        """
        delete the temporary file and directory
        """
        for f in os.listdir(self.tmpdir):
            os.remove(os.path.join(self.tmpdir, f))
        os.rmdir(self.tmpdir)

    def testSizecheck(self):
        """
        Verify that the size test detects empty files.
        """
        self.assertTrue(self.dew.errors == {},
                        'start sizecheck - errors is not empty')

        self.dew.sizecheck(self.test_file)
        self.dew.sizecheck(self.empty_file)
        self.dew.sizecheck('vos:jsaops/unittest/empty_file.txt')

        self.assertTrue(len(self.dew.errors) == 2,
                        'wrong number of errors: ' +
                        str(len(self.dew.errors)))
        self.assertTrue(len(self.dew.warnings) == 0,
                        'wrong number of warnings: ' +
                        str(len(self.dew.warnings)))

        empty_files = [self.empty_file, 'vos:jsaops/unittest/empty_file.txt']
        for filename in self.dew.errors:
            self.assertTrue(filename in empty_files,
                            filename + ' not in ' + repr(empty_files))
            self.assertEqual(self.dew.errors[filename],
                             ['file has length = 0'])

    def testNamecheck(self):
        """
        Verify that the namecheck test records only badly named files.
        """
        self.assertTrue(self.dew.errors == {},
                        'start namecheck - errors is not empty')

        self.dew.namecheck(self.test_file)
        self.dew.namecheck(self.archive_file)
        self.dew.namecheck(self.bogus_file)

        self.assertTrue(len(self.dew.errors) == 1,
                        'wrong number of errors: ' +
                        str(len(self.dew.errors)))
        self.assertTrue(len(self.dew.warnings) == 0,
                        'wrong number of warnings: ' +
                        str(len(self.dew.warnings)))

        self.assertEqual(self.dew.errors.keys(), [self.bogus_file])
        self.assertTrue(re.match(r'namecheck failed',
                                 self.dew.errors[self.bogus_file][0]))

    def testInArchiveErrorWarning(self):
        """
        Verify that in_archive records files that are/are not in the archive.
        """
        self.assertTrue(self.dew.errors == {},
                        'start in_archive_error_warn - errors is not empty')
        self.assertTrue(self.dew.warnings == {},
                        'start in_archive_error_warn - errors is not empty')

        real_file = 's8a20131001_00003_0001'
        error_warning = {True: 'error',
                         False: 'warning'}

        self.dew.in_archive(real_file, error_warning)
        self.dew.in_archive(self.test_file, error_warning)

        self.assertTrue(len(self.dew.errors) == 1,
                        'wrong number of errors: ' +
                        str(len(self.dew.errors)))
        self.assertTrue(len(self.dew.warnings) == 1,
                        'wrong number of warnings: ' +
                        str(len(self.dew.warnings)))

        self.assertEqual(self.dew.errors.keys(), [real_file])
        self.assertTrue(re.match(r'name conflict',
                                 self.dew.errors[real_file][0]))

        self.assertEqual(self.dew.warnings.keys(), [self.test_file])
        self.assertTrue(re.match(r'file is not present',
                                 self.dew.warnings[self.test_file][0]))

    def testInArchiveWarningError(self):
        """
        Verify that in_archive records files that are/are not in the archive.
        """
        self.assertTrue(self.dew.errors == {},
                        'start in_archive_replace - errors is not empty')

        real_file = 's8a20131001_00003_0001'
        warning_error = {True: 'warning',
                         False: 'error'}

        self.dew.in_archive(real_file, warning_error)
        self.dew.in_archive(self.test_file, warning_error)

        self.assertTrue(self.dew.error_count() == 1,
                        'wrong number of errors: ' +
                        str(len(self.dew.errors)))
        self.assertTrue(len(self.dew.warnings) == 1,
                        'wrong number of warnings: ' +
                        str(len(self.dew.warnings)))

        self.assertEqual(self.dew.errors.keys(), [self.test_file])
        self.assertTrue(re.match(r'expected file not found',
                                 self.dew.errors[self.test_file][0]))

        self.assertEqual(self.dew.warnings.keys(), [real_file])
        self.assertTrue(re.match(r'existing file',
                                 self.dew.warnings[real_file][0]))

    def testInArchiveNoneNone(self):
        """
        Verify that in_archive records files that are/are not in the archive.
        """
        self.assertTrue(self.dew.errors == {},
                        'start in_archive_replace - errors is not empty')

        real_file = 's8a20131001_00003_0001'
        none_none = {True: None,
                     False: None}

        self.dew.in_archive(real_file, none_none)
        self.dew.in_archive(self.test_file, none_none)

        self.assertTrue(self.dew.error_count() == 0,
                        'wrong number of errors: ' +
                        str(len(self.dew.errors)))
        self.assertTrue(len(self.dew.warnings) == 0,
                        'wrong number of warnings: ' +
                        str(len(self.dew.warnings)))

    def testFitsverify(self):
        """
        Verify that fitsverify records files that report errors
        """
        self.assertTrue(self.dew.errors == {},
                        'start in_archive_replace - errors is not empty')

        self.dew.fitsverify(self.test_file)
        self.dew.fitsverify(self.bogus_file)
        self.dew.fitsverify(self.empty_file)
        bad_files = (self.bogus_file, self.empty_file)

        self.assertTrue(self.dew.error_count() == 1,
                        'wrong number of errors: ' +
                        str(self.dew.error_count()))
        for filename in self.dew.errors.keys():
            self.assertTrue(filename in bad_files,
                            filename + ' is not in ' + repr(bad_files))
            self.assertTrue(re.match(r'fitsverify reported',
                                     self.dew.errors[filename][0]))

    def testExpectKeyword(self):
        """
        Verify that expect_keyword records files missing mandatory headers
        """
        self.assertTrue(self.dew.errors == {},
                        'start in_archive_replace - errors is not empty')

        header = fits.getheader(self.test_file, 0)
        self.dew.expect_keyword(self.test_file, 'ASN_ID', header, True)
        self.dew.expect_keyword(self.test_file, 'PRODID', header, True)
        self.dew.expect_keyword(self.test_file, 'DPDATE', header, True)
        missing_headers = r'(ASN_ID|PRODID)'

        self.assertTrue(self.dew.error_count() == 1,
                        'wrong number of files with errors: ' +
                        str(self.dew.error_count()))
        self.assertTrue(len(self.dew.errors[self.test_file]) == 2,
                        'wrong number of errors in file: ' +
                        str(len(self.dew.errors[self.test_file])))
        for msg in self.dew.errors[self.test_file]:
            self.assertTrue(re.search(missing_headers, msg))

    def testRestrictedValues(self):
        """
        Verify that restricted_value records files with invalid header values
        """
        self.assertTrue(self.dew.errors == {},
                        'start in_archive_replace - errors is not empty')

        header = fits.getheader(self.test_file, 0)
        self.dew.restricted_value(self.test_file, 'COLLECT', header,
                                  ['TEST', 'JCMT'])
        self.dew.restricted_value(self.test_file, 'PRODUCT', header,
                                  ['X', 'Y', 'Z'])
        bad_headers = r'(PRODUCT)'

        self.assertTrue(len(self.dew.errors[self.test_file]) == 1,
                        'wrong number of errors: ' + str(len(self.dew.errors)))
        for msg in self.dew.errors[self.test_file]:
            self.assertTrue(re.search(bad_headers, msg))

    def testReport(self):
        """
        Verify that report does not crash
        """
        header = fits.getheader(self.test_file, 0)
        self.dew.expect_keyword(self.test_file, 'ASN_ID', header, True)
        self.dew.expect_keyword(self.test_file, 'PRODID', header, True)
        self.dew.expect_keyword(self.test_file, 'DPDATE', header, True)
        self.dew.restricted_value(self.test_file, 'COLLECT', header,
                                  ['TEST', 'JCMT'])
        self.dew.restricted_value(self.test_file, 'PRODUCT', header,
                                  ['X', 'Y', 'Z'])
        self.dew.report()

    def testGather(self):
        """
        Verify that report does not crash
        """
        with self.dew.gather() as dew:
            header = fits.getheader(self.test_file, 0)
            dew.expect_keyword(self.test_file, 'ASN_ID', header, True)
            dew.expect_keyword(self.test_file, 'PRODID', header, True)
            dew.expect_keyword(self.test_file, 'DPDATE', header, True)
            dew.restricted_value(self.test_file, 'COLLECT', header,
                                 ['TEST', 'JCMT'])
            dew.restricted_value(self.test_file, 'PRODUCT', header,
                                 ['X', 'Y', 'Z'])
