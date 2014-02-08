#!/usr/bin/env python
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
#*   Script Name:    file_container.py
#*
#*   Purpose:
#*    Container class that holds a list of paths to files to ingest
#*
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
__author__ = "Russell O. Redman"


import os.path

from tools4caom2 import __version__
from tools4caom2.basecontainer import basecontainer

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
                    raise basecontainer.ContainerError('File not found: ' + f)

        if file_count == 0:
            raise basecontainer.ContainerError(
                    'ERROR: adfile ' + adfile + ' contains no valid files')

