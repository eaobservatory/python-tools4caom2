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

__author__ = "Russell O. Redman"

import logging
import os
import os.path
import re
import time
from vos import Client

from tools4caom2 import __version__
from tools4caom2.container.base import basecontainer
from tools4caom2.data_web_client import data_web_client
from tools4caom2.error import CAOMError
from tools4caom2.validation import CAOMValidationError

__doc__ = """
The vos_container class reads from a text file a list of AD URIs that
reference the files to ingest.

Version: """ + __version__.version

logger = logging.getLogger(__name__)


class vos_container(basecontainer):
    def __init__(self,
                 vosroot,
                 archive_name,
                 ingest,
                 working_directory,
                 validation,
                 vosclient,
                 dataweb,
                 make_file_id):
        """
        Reads a list of files from a VOspace directory and subdirectories.
        The filterfunc should be called to exclude files that should not be
        ingested.  If the files have already been copied into the archive,
        the flag should be set True; otherwise the files will be fetched
        directly from the VOspace.  In either case, the files will be extracted
        into working_directory and after use will be deleted again.

        Arguments:
        data_web_client:   a tools4caom2.data_web_client object
        vosroot:           a uri pointing to a VOspace directory
        archive_name:      archive that will contain copies of the files
        ingest:            True if ingesting files from AD, False otherwise
        validation:        validation object
        working_directory: directory to hold files from AD
        make_file_id:      function that turns a file uri/url/path into a
                           file_id
        """
        basecontainer.__init__(self, re.sub(r'[:/]', '-', vosroot))
        self.dataweb = dataweb
        self.archive_name = archive_name
        self.ingest = ingest
        self.validation = validation
        self.working_directory = working_directory
        self.make_file_id = make_file_id
        self.vosclient = vosclient

        if self.vosclient.access(vosroot) and self.vosclient.isdir(vosroot):
            self.vosroot = vosroot
        else:
            raise CAOMError('vos does not exist: ' + vosroot)

        if os.path.isdir(working_directory):
            self.directory = os.path.abspath(working_directory)
        else:
            raise CAOMError('working_directory is not a directory: ' +
                            working_directory)

        self.vospath = {}

        filecount = self.readvos(vosroot)
        if filecount == 0:
            logger.warning('vos contains no ingestible files: %s', vosroot)

    def readvos(self, uri):
        """
        Call recursively to read the list of files from a VOspace
        """
        pathlist = [uri + '/' + f for f in self.vosclient.listdir(uri,
                                                                  force=True)]
        logger.debug('pathlist = %s', repr(pathlist))
        dirlist = sorted([f for f in pathlist if self.vosclient.isdir(f)])
        logger.debug('dirlist = %s', repr(dirlist))
        filelist = sorted([f for f in pathlist if self._is_valid_file(f)])
        logger.debug('filelist = %s', repr(filelist))

        filecount = 0
        for f in filelist:
            file_id = self.make_file_id(f)
            filename = os.path.basename(f)
            if file_id in self.vospath:
                logger.error('%s: duplicate file_id = %s for %s',
                             f, file_id, self.vospath[file_id])
                raise CAOMError('Duplicate file in VOS container')
            else:
                filecount += 1
                self.vospath[file_id] = f
                self.filedict[file_id] = os.path.join(self.directory,
                                                      filename)

        for d in dirlist:
            filecount += self.readvos(d)

        return filecount

    def _is_valid_file(self, pathname):
        """
        Determine whether a given file should be included in the
        results from readvos or not.  Returns True if so, False
        otherwise.
        """

        if not self.vosclient.isfile(pathname):
            return False

        try:
            self.validation.check_size(pathname)
            self.validation.check_name(pathname)
        except CAOMValidationError:
            return False
        else:
            return True

    def get(self, file_id):
        """
        Fetch a file from ad into the working directory

        Arguments:
        file_id : The file_id to extract from the archive
        """
        if file_id not in self.filedict:
            raise CAOMError('requesting bad file_id: ' + file_id +
                            ' from ' + repr(self.file_id_list()))

        # This fetches only the header from the primary HDU, which
        # should result in significant performance improvements
        if self.ingest:
            # file should already be in AD
            # This gets ONLY the primary header
            logger.info('vos_container.get using data_web_client %s %s',
                        self.archive_name, file_id)
            filepath = self.dataweb.get(self.archive_name,
                                        file_id,
                                        params=data_web_client.PrimaryHEADER)
            if not filepath:
                raise CAOMError('could not get ' + file_id + ' from ' +
                                self.archive_name)

            self.filedict[file_id] = filepath
        else:
            # fetch the whole file from vos
            filepath = self.filedict[file_id]
            logger.info('vos_container.get using vosclient %s',
                        self.vospath[file_id])
            filesize = self.vosclient.copy(self.vospath[file_id], filepath)
            if not filesize:
                raise CAOMError('could not get ' + file_id + ' from ' +
                                filepath)

        logger.debug('filepath = %s', filepath)

        return filepath

    def uri(self, file_id):
        """
        Return the vos uri for a file_id

        Arguments:
        file_id : The file_id for which the vos uri is required
        """
        if file_id not in self.vospath:
            raise CAOMError('requesting bad file_id: ' + file_id +
                            ' from ' + repr(self.file_id_list()))
        return self.vospath[file_id]

    def cleanup(self, file_id):
        """
        Clean up deletes the file from the working directory

        Arguments:
        file_id : file_id of the file to delete
        """
        os.remove(self.filedict[file_id])
