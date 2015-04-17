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

__author__ = "Russell O. Redman"

import logging
import os.path

from tools4caom2 import __version__
from tools4caom2.error import CAOMError
from tools4caom2.container.base import basecontainer

__doc__ = """
The file_container class is holds a list of files to ingest referenced by
their paths on the disk.  The file_container does NOT delete the file after
it has been used.

Version: """ + __version__.version


class filelist_container(basecontainer):
    def __init__(self,
                 listname,
                 list_of_files,
                 filterfunc,
                 make_file_id):
        """
        Initialize an instance of a container holding a list of files.
        All of the files in the list must exist, and an exception will be
        raised if any do not.

        Arguments:
        list_of_files: a list of file names
        filterfunc: returns True if file name should be ingested
        make_file_id: returns the file_id corresponding to file name
        """
        basecontainer.__init__(self, listname)
        file_count = 0
        for f in list_of_files:
            if not filterfunc or filterfunc(f):
                filepath = \
                    os.path.abspath(os.path.expanduser(os.path.expandvars(f)))
                file_id = make_file_id(os.path.basename(filepath))
                if os.path.exists(filepath):
                    self.filedict[file_id] = filepath
                    file_count += 1
                else:
                    raise CAOMError('File not found: ' + f)

        if file_count == 0:
            raise CAOMError('filelist ' + listname +
                            ' contains no valid files')
