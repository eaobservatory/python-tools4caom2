# Copyright (C) 2014-2015 Science and Technology Facilities Council.
# Copyright (C) 2015 East Asian Observatory.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import

import os
import re
import shutil
import tempfile
from unittest import TestCase

from astropy.io import fits
import numpy

from tools4caom2.validation import CAOMValidation, CAOMValidationError
from tools4caom2.util import make_file_id

from .write_fits import write_fits


class test_validation(TestCase):
    def setUp(self):
        """
        Create a temp directory and validation object.  The
        temporary directory must be shared amongst several subprocesses, so we
        cannot use mkdtemp.
        """
        self.tmpdir = tempfile.mktemp(dir='/tmp')
        os.mkdir(self.tmpdir)

        fileid_regexes = [
            re.compile(r'^test_.*\.fits$'),
            re.compile(r'^archive_.*\.fits$'),
            re.compile(r'^TEST_.*\.png$'),
        ]

        self.validation = CAOMValidation(
            'JCMT', fileid_regexes, make_file_id)

        # Create fits files with suitable test headers
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
        Delete the temporary directory.
        """

        shutil.rmtree(self.tmpdir)

    def testCheckSize(self):
        """
        Verify that the size test detects empty files.
        """

        self.validation.check_size(self.test_file)

        with self.assertRaises(CAOMValidationError):
            self.validation.check_size(self.empty_file)

        with self.assertRaises(CAOMValidationError):
            self.validation.check_size('vos:jsaops/unittest/empty_file.txt')

    def testCheckName(self):
        """
        Verify that the name-check test rejects only badly named files.
        """

        self.validation.check_name(self.test_file)
        self.validation.check_name(self.archive_file)

        with self.assertRaises(CAOMValidationError):
            self.validation.check_name(self.bogus_file)

    def testIsInArchive(self):
        """
        Verify that is_in_archive identifies files that are/are not in the
        archive.
        """

        real_file = 's8a20131001_00003_0001.sdf'

        self.validation.is_in_archive(real_file)

        with self.assertRaises(CAOMValidationError):
            self.validation.is_in_archive(self.test_file)

    def testVerifyFits(self):
        """
        Verify that verify_fits rejects files that report errors.
        """

        self.validation.verify_fits(self.test_file)
        self.validation.verify_fits(self.bogus_file)

        with self.assertRaises(CAOMValidationError):
            self.validation.verify_fits(self.empty_file)

    def testExpectKeyword(self):
        """
        Verify that expect_keyword rejects files missing mandatory headers.
        """

        header = fits.getheader(self.test_file, 0)

        self.validation.expect_keyword(self.test_file, 'DPDATE', header)

        with self.assertRaises(CAOMValidationError):
            self.validation.expect_keyword(self.test_file, 'ASN_ID', header)

        with self.assertRaises(CAOMValidationError):
            self.validation.expect_keyword(self.test_file, 'PRODID', header)

    def testRestrictedValues(self):
        """
        Verify that restricted_value rejects files with invalid header values.
        """

        header = fits.getheader(self.test_file, 0)

        self.validation.restricted_value(self.test_file, 'COLLECT', header,
                                         ['TEST', 'JCMT'])

        with self.assertRaises(CAOMValidationError):
            self.validation.restricted_value(self.test_file, 'PRODUCT', header,
                                             ['X', 'Y', 'Z'])
