# Copyright (C) 2014-2015 Science and Technology Facilities Council.
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

import logging
import os.path


def configure_logger(level=logging.DEBUG):
    logging.basicConfig(
        datefmt='%Y-%m-%dT%H:%M:%S',
        format='%(levelname)-8s %(asctime)s %(name)s %(message)s',
        level=level)


def make_file_id(filepath):
    """
    A routine to convert a filename to the corresponding file_id used to
    identify the file in CADC storage.  This picks out the basename from
    the path and forces the name into lower case.

    Arguments:
    filepath: path to the file

    Returns:
    file_id: string used to identify the file in storage
    """

    return os.path.basename(filepath).lower()
