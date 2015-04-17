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

from __future__ import absolute_import, division, print_function

import logging
import os
import re
import subprocess

try:
    from astropy.io import fits
except:
    import pyfits as fits

from vos.vos import Client as vosclient

from tools4caom2.data_web_client import data_web_client
from tools4caom2.error import CAOMError

logger = logging.getLogger(__name__)


class CAOMValidationError(CAOMError):
    """
    Class for errors raised as a result of validation failures.
    """

    pass


class CAOMValidation:
    def __init__(self, outdir, archive, fileid_regex_dict, make_file_id):
        """Construct CAOM validation object.

        Arguments:
        outdir            : local directory for temporary files
        archive           : name of archive
        fileid_regex_dict : dictionary keyed on extension containing a list
                            of compiled regex objects matching valid file_ids
        make_file_id      : function returning a file_id from a filename
        """

        self.archive = archive
        self.fileid_regex_dict = fileid_regex_dict
        self.make_file_id = make_file_id

        self.data_web_client = data_web_client(outdir)
        self.vosclient = vosclient()

    def check_size(self, filename):
        """
        Raise an error if the file is empty.
        """

        if re.match(r'vos:', filename):
            length = int(self.vosclient.getNode(filename).getInfo()['size'])
        elif os.path.isfile(filename):
            length = os.path.getsize(filename)

        if not length:
            raise CAOMValidationError('file {0} has zero length'.format(
                filename))

    def check_name(self, filename):
        """
        Raise an error if the filename is unacceptable.
        """

        ext = os.path.splitext(filename)[1].lower()
        file_id = self.make_file_id(filename)

        if ext in self.fileid_regex_dict:
            for regex in self.fileid_regex_dict[ext]:
                if regex.match(file_id):
                    return

        raise CAOMValidationError('file {0} failed namecheck'.format(filename))

    def is_in_archive(self, filename):
        """
        Return true if the file is in the archive.
        """

        file_id = self.make_file_id(filename)

        if self.data_web_client.info(self.archive, file_id):
            return

        raise CAOMValidationError(
            'file {0} is not in the archive'.format(filename))

    def verify_fits(self, filename):
        """
        Raise an exception if fitsverify finds errors for the given file.
        """

        try:
            # fitsverify will return a non-zero exit code if there were
            # errors or warnings, but can fail for other reasons as well
            output = subprocess.check_output(['fitsverify',
                                              '-q',
                                              filename])
        except subprocess.CalledProcessError as e:
            # absorb all exceptions, but such files are recorded as
            # causing errors
            output = str(e.output)
        except:
            output = type(e) + ': 1 errors'

        if re.search(r'\s*verification OK', output):
            error_count = '0'
        else:
            error_count = re.sub(r'.*?\s(\d+) errors.*', r'\1', output)

        if int(error_count):
            raise CAOMValidationError('file {0} failed fitsverify'.format(
                filename))

    def expect_keyword(self, filename, key, header):
        """
        Raise an exception if a key is not in the header.

        Arguments:
        filename : filesystem path to a file
        header   : FITS header from the primary HDU
        key      : mandatory keyword
        """

        if (key not in header) or (header[key] == fits.card.UNDEFINED):
            raise CAOMValidationError('file {0} lacks header {1}'.format(
                filename, key))

    def restricted_value(self, filename, key, header, value_list):
        """
        Raise an exception if a header isn't in the set of acceptable
        values.

        Arguments:
        filename   : filesystem path to a file
        key        : keyword whose value must be in the value_list
        header     : FITS header from the primary HDU
        value_list : list of acceptable values
        """

        if key in header and header[key] != fits.card.UNDEFINED:
            if header[key] in value_list:
                return

            raise CAOMValidationError(
                'file {0} header {1} ({2}) should be in {3!r}'.format(
                    filename, key, header[key], value_list))

        raise CAOMValidationError(
            'file {0} lacks restricted header {1}'.format(filename, key))
