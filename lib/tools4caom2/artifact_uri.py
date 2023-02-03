# Copyright (C) 2023 East Asian Observatory
# All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful,but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,51 Franklin
# Street, Fifth Floor, Boston, MA  02110-1301, USA

from tools4caom2.error import CAOMError


def extract_artifact_uri_filename(uri, archive):
    """
    Extract filename from URI, checking that the archive
    part of the URI is as expected.
    """

    prefix = 'cadc:{}/'.format(archive)
    if uri.startswith(prefix):
        return uri[len(prefix):]

    raise CAOMError(
        'Artifact URI "{}" not of expected format'.format(uri))


def make_artifact_uri(filename, archive):
    """
    Construct artifact URI for a filename or filename pattern.
    """

    return 'cadc:{}/{}'.format(archive, filename)
