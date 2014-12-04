#!/usr/bin/env python2.7
__author__ = "Russell O. Redman"

import logging
import os
import os.path
import stat
import tarfile

from tools4caom2 import __version__
from tools4caom2.basecontainer import basecontainer

__doc__ = """
The tarfile_container class holds a list of files to ingest that are stored
in a tar file, which can be gzipped.

Version: """ + __version__.version

class tarfile_container(basecontainer):
    def __init__(self,
                 log,
                 tarfilepath,
                 working_directory,
                 filterfunc,
                 make_file_id):
        """
        Initialize an instance of a container from a tar file holding a set of
        files.  Within the tar file the files can reside in subdirectories
        nested arbitrarily deeply.  Upon extraction, the files will be
        placed in working_directory, which must already exist.  There is no
        filtering of the files, nor any ordering.  If several files have the
        same file_id but different extensions, the last-encountered file will
        be the one referenced by the container.

        The files will be extracted from the tar file into working_directory,
        and after use will be deleted again.

        Arguments:
        tarfilepath:       the path to a tar file
        working_directory: the directory into which files will be extracted
        test:              if True, do not access archive or database 
                           present for compatability with other containers, 
                           but ignored for tarfile_containers
        make_file_id:      function that converst a file name to a file_id
        """
        basecontainer.__init__(self, log, os.path.basename(tarfilepath))

        if os.path.isdir(working_directory):
            self.directory = os.path.abspath(working_directory)
        else:
            self.log.console('Working directory is not a directory: ' +
                             working_directory,
                             logging.ERROR)

        self.tarfilemember = {}
        self.tarfilepath = tarfilepath

        file_count = 0
        if tarfile.is_tarfile(self.tarfilepath):
            self.TAR = tarfile.open(self.tarfilepath, 'r')
            for f in self.TAR.getnames():
                filename = os.path.basename(f)
                if not filterfunc or filterfunc(filename):
                    file_id = make_file_id(filename)
                    self.tarfilemember[file_id] = f
                    self.filedict[file_id] = os.path.join(self.directory,
                                                          filename)
                    file_count += 1
            self.TAR.close()
        else:
            self.log.console('tarfile is not a tar file: ' +
                             tarfilepath,
                             logging.ERROR)
        self.TAR = None
        
        if file_count == 0:
            self.log.console('tar file ' + tarfilepath + 
                             ' contains no valid files',
                             logging.ERROR)

    def get(self, file_id):
        """
        Extract a particular file identified by file_id from the tar file
        into the working directory.
        
        Arguments:
        file_id:  the key for the file
        """
        if not self.TAR:
            self.TAR = tarfile.open(self.tarfilepath, 'r')

        if file_id in self.tarfilemember and file_id in self.filedict:
            self.TAR.extract(self.tarfilemember[file_id],
                                 self.directory)
            return self.filedict[file_id]
        else:
            self.log.console(file_id + ' is not a member of the tar file ' +
                             self.tarfilepath,
                             logging.ERROR)

    def cleanup(self, file_id):
        """
        Delete the file from the working directory.
        
        Arguments:
        file_id:  the key for the file
        """
        os.chmod(self.filedict[file_id],
                 stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
        os.remove(self.filedict[file_id])

    def close(self):
        """
        Close the tar file for this container if it is open

        Arguments:
        <none>
        """
        if self.TAR:
            self.TAR.close()
        self.TAR = None


