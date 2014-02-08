#/*+
#************************************************************************
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#*
#* (c) 2013  .                      (c) 2013
#* National Research Council        Conseil national de recherches
#* Ottawa, Canada, K1A 0R6          Ottawa, Canada, K1A 0R6
#* All rights reserved              Tous droits reserves
#*
#* NRC disclaims any warranties,    Le CNRC denie toute garantie
#* expressed, implied, or statu-    enoncee, implicite ou legale,
#* tory, of any kind with respect   de quelque nature que se soit,
#* to the software, including       concernant le logiciel, y com-
#* without limitation any war-      pris sans restriction toute
#* ranty of merchantability or      garantie de valeur marchande
#* fitness for a particular pur-    ou de pertinence pour un usage
#* pose.  NRC shall not be liable   particulier.  Le CNRC ne
#* in any event for any damages,    pourra en aucun cas etre tenu
#* whether direct or indirect,      responsable de tout dommage,
#* special or general, consequen-   direct ou indirect, particul-
#* tial or incidental, arising      ier ou general, accessoire ou
#* from the use of the software.    fortuit, resultant de l'utili-
#*                                  sation du logiciel.
#*
#************************************************************************
#*
#*   Script Name:    basecontainer.py
#*
#*   Purpose:
#*    Base class for a set of container classes that hold lists of files
#*    to ingest
#*
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
__author__ = "Russell O. Redman"

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
        basecontainer.__init__(self, os.path.basename(tarfilepath))

        if os.path.isdir(working_directory):
            self.directory = os.path.abspath(working_directory)
        else:
            raise basecontainer.ContainerError('ERROR: not a directory: ' +
                                               working_directory)

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
            raise basecontainer.ContainerError('ERROR: not a tar file: ' +
                                               tarfilepath)
        self.TAR = None
        
        if file_count == 0:
            raise basecontainer.ContainerError(
                'ERROR: tar file ' + tarfilepath + ' contains no valid files')

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
            raise basecontainer.ContainerError(
                'ERROR: ' + file_id + ' is not a member of the tar file ' +
                self.tarfilepath)

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


