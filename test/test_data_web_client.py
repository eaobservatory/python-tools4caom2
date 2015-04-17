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

__author__ = "Russell O. Redman"

from datetime import datetime
import logging
import numpy as np
import os
import os.path
from astropy.io import fits
import re
import tempfile
import unittest

from tools4caom2.data_web_client import data_web_client


class testDataWebService(unittest.TestCase):
    def setUp(self):
        """
        Create a temp directory and data_web_client object
        """
        self.tmpdir = tempfile.mkdtemp()
        self.web_service = data_web_client(self.tmpdir)

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
        Create FITS file put into TEST archive, then verify that it can be
        found
        """
        filename = 'data_web_client_test.fits'
        file_id, ext = os.path.splitext(filename)
        filepath = os.path.join(self.tmpdir, filename)

        # Create a test with unique metadata
        data = np.arange(100)
        hdu = fits.PrimaryHDU(data)
        hdulist = fits.HDUList([hdu])
        hdulist.writeto(filepath)

        s = self.web_service.put(filepath, 'TEST', file_id, adstream='test')
        self.assertTrue(s, 'failed to put ' + filepath + ' to TEST with ' +
                        ' file_id = ' + file_id)

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
        hdu = fits.PrimaryHDU(data)
        hdu.header['DATE'] = mydate
        hdulist = fits.HDUList([hdu])
        hdulist.writeto(filepath)

        s = self.web_service.put(filepath, 'TEST', file_id, adstream='test')
        self.assertTrue(s, 'failed to put ' + filepath + ' to TEST with ' +
                        ' file_id = ' + file_id)

        d = self.web_service.get('TEST',
                                 file_id,
                                 params=data_web_client.PrimaryHEADER)
        header_copy = fits.getheader(d, 0)
        self.assertTrue('DATE' in header_copy,
                        'The DATE header is missing from ' + d)
        self.assertEqual(mydate, header_copy['DATE'],
                         'The DATE header ' + header_copy['DATE'] +
                         ' does not equal mydate=' + mydate)
