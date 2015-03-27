# TODO: insert copyright
# TODO: insert license
# Code extracted from tools4caom2.caom2ingest.

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
