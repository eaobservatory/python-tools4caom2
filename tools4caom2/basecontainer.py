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


from contextlib import contextmanager
import exceptions

from tools4caom2 import __version__

__doc__ = """
The basecontainer class is a base class that defines the interface for a
set of container classes that hold lists of files to ingest.

Version: """ + __version__.version

#************************************************************************
#* Container classes to hold lists of files to be ingested
#* basecontainer - base class for all such containers
#* filelist_container - container for a list of files (from the command line)
#* tarfile_container - container for a tar file of files to ingest
#* adfile_container - container for a text file of ad URIs
#************************************************************************
class basecontainer(object):
    """
    Base class for a file container, which is intended to be used in a with
    statement. The methods used to access files in the container are:

    file_id_list(self) : return a list of the file_id's in the container
                         in arbitrary order
    get(self, file_id) : if file_id is in the container, copy it to the
                         working directory (if necessary) and return its
                         absolute filepath
    cleanup(self) :      delete the file from the disk if necessary
    use(self, file_id) : get file_id from ad into the working directory, then
                         yield for processing and call cleanup when done

    The correct idiom to use a container is
    mycontainer = container(...)
    try:
        file_id_list = mycontainer.file_id_list()
        <filter and sort file_id_list>
        for file_id in file_id_list:
            with mycontainer.use(file_id) as f:
                <do something with file f>
    finally:
        mycontainer.close()
    """

    def __init__(self, name, log):
        """
        Initialize the basecontainer class.  Every container must have a
        filedict whose keys are the file_id's for the set of files in the
        container.

        Arguments:
        <None>
        """
        self.name = name
        self.log = log
        self.filedict = {}

    def file_id_list(self):
        """
        Return the file_id list in the basecontainer.

        Arguments:
        <none>
        """
        return self.filedict.keys()

    def get(self, file_id):
        """
        Fetch a copy of the file corresponding to file_id to a known location
        on the disk and return that location to the calling program.

        Argument:
        file_id : the file_id of the file in the container

        This method should not be used directly.  Instead, call the use
        contextmanager in a with statement to ensure that cleanup happens
        reliably.
        """
        return self.filedict[file_id]

    def cleanup(self, file_id):
        """
        Clean up the file if necessary when it is no longer needed.
        Argument:
        file_id : the file_id of the file to be cleaned up

        This method should not be used directly.  Instead, call the use
        contextmanager in a with statement to ensure that cleanup happens
        reliably.
        """
        pass

    @contextmanager
    def use(self, file_id):
        """
        A contextmanager that fetches a file indexed by its file_id from a
        container, and cleans up the file if necessary after it is no longer
        in use.  For example:
            with thiscontainer.use(file_id) as f:
                <use the file f until done>
        """
        try:
            yield self.get(file_id)
        finally:
            self.cleanup(file_id)

    def close(self):
        """
        Close the container

        Arguments:
        <none>
        """
        pass
