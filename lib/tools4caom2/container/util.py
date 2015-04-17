# Copyright (C) 2014 Science and Technology Facilities Council.
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

import os.path


def fitsfilter(filename):
    """
    Return True if this file should be ingested, False otherwise.
    By default, only ingest FITS files.  The filter will only be applied
    to files in a directory, tar file or file list, not to file_id's in
    an AD file.

    Arguments:
    filename : the file name to check for validity
    This is a static method taking exactly one argument.
    """
    return (os.path.splitext(filename)[1].lower() in
            ['.fits', '.fit'])


def nofilter(filename):
    """
    Return True always, so no files are filered out.

    Arguments:
    filename : the file name to check for validity
    This is a static method taking exactly one argument.
    """
    return True
