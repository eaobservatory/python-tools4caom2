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
#*   Script Name:    adfile_container.py
#*
#*   Purpose:
#*    Container class that holds a list of files to ingest that are
#*    referenced by their AD uri's
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
__author__ = "Russell O. Redman"

import commands
import os
import os.path
import re
import time

from tools4caom2 import __version__
from tools4caom2.basecontainer import basecontainer

__doc__ = """
The adfile_container class reads from a text file a list of AD URIs that 
reference the files to ingest.

Version: """ + __version__.version

class adfile_container(basecontainer):
    def __init__(self, adfile, working_directory, filterfunc):
        """
        An adfile is a text file containing a list of URIs for files in ad
        in the format "^\s*(ad:[A-Z]+/(a-zA-Z.-_]+)(\s.*)?$", i.e. optional
        whitespace, the URI and optionally whitespace followed by anything.
        For example:
          ad:JCMT/jcmth20110811_00044_01_reduced001_nit_000 Reduced ACSIS file
          ad:JCMT/jcmth20110811_00044_01_rsp_nit_000    Corresponding rsp file
        Lines not matching that format will be ignored.  Any URIs found in
        the text file will be checked to verify that the file exists in ad
        before the file_id is added to the filedict.

        The files will be extracted from AD into working_directory and after
        use will be deleted again.

        To quickly distinguish adfiles from other kinds of text files (e.g.
        catalogs that might need to be ingested), an adfile must have the
        extension ".ad".
        
        Arguments:
        adfile:            path to file containing a list of adURI's
        working_directory: directory to hold files from AD
        filterfunc:        returns True if a filename should be ingested
        """
        if not os.path.exists(adfile):
            raise basecontainer.ContainerError(
                    'ERROR: ad_file does not exist: ' + adfile)
        
        basecontainer.__init__(self, os.path.basename(adfile))

        if os.path.isdir(working_directory):
            self.directory = os.path.abspath(working_directory)
        else:
            raise basecontainer.ContainerError('ERROR: not a directory: ' +
                                               working_directory)

        self.ad_uri = {}
            
        with open(adfile, 'r') as ADF:
            filecount = 0
            for line in ADF:
                match = re.search(r'ad:([A-Z]+)/([a-zA-Z0-9.\-_]+)',
                                 line)
                if match:
                    (archive, file_id) = match.group(1, 2)
                    cmd = 'adInfo -a %s -q -fileName %s' % \
                          (archive, file_id)

                    # work-around for adInfo bug - retry up to 3 times with
                    # increasing delays
                    numtries = 0
                    status = 1
                    while numtries < 3 and status:
                        status, adfilename = commands.getstatusoutput(cmd)
                        if status:
                            numtries += 1
                            time.sleep(1.0 * numtries)
                    if status:
                        raise basecontainer.ContainerError(
                            'adInfo fails for %s in %s with status=%d'
                            ' after %d tries' %
                            (file_id, adfile, status, numtries))
                    if not filterfunc or filterfunc(adfilename):
                        self.filedict[file_id] = os.path.join(self.directory,
                                                              adfilename)
                        self.ad_uri[file_id] = archive

                        filecount += 1
            if filecount == 0:
                raise basecontainer.ContainerError(
                    'ERROR: adfile ' + adfile + ' contains no valid ad URIs')

    def get(self, file_id):
        """
        Fetch a file from ad into the working directory

        Arguments:
        file_id : The file_id to extract from the archive
        """
        if file_id not in self.ad_uri:
            raise basecontainer.ContainerError('ERROR: ' + file_id +
                                               ' not in ' +
                                               repr(self.file_id_list()))

        cmd = 'cd %s ; adGet -a %s %s' % (self.directory,
                                          self.ad_uri[file_id],
                                          file_id)

        status, output = commands.getstatusoutput(cmd)
        if status:
            raise basecontainer.ContainerError('ERROR: ' + cmd +
                                               ' returned: "' + output + '"')
        return self.filedict[file_id]

    def cleanup(self, file_id):
        """
        Clean up deleted the file from the working directory

        Arguments:
        file_id : file_id of the file to delete
        """
        os.remove(self.filedict[file_id])
