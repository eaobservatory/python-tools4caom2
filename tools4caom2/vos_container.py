#!/usr/bin/env python

__author__ = "Russell O. Redman"

import commands
import logging
import os
import os.path
import re
import time
from vos import Client

from tools4caom2 import __version__
from tools4caom2.basecontainer import basecontainer
from tools4caom2.data_web_client import data_web_client

__doc__ = """
The vos_container class reads from a text file a list of AD URIs that 
reference the files to ingest.

Version: """ + __version__.version

class vos_container(basecontainer):
    def __init__(self, 
                 log, 
                 data_web_client, 
                 vosroot, 
                 archive,
                 in_archive,
                 working_directory, 
                 filterfunc,
                 make_file_id):
        """
        Reads a list of files from a VOspace directory and subdirectories.
        The filterfunc should be called to exclude files that should not be 
        ingested.  If the files have already been copied into the archive,
        the flag should be set True; otherwise the files will be fetched
        directly from the VOspace.  In either case, the files will be extracted 
        into working_directory and after use will be deleted again.

        Arguments:
        log:               a tools4caom2.logger object
        data_web_client:   a tools4caom2.data_web_client object
        vosroot:           a uri pointing to a VOspace directory
        archive_name:      archive that will contain copies of the files
        in_archive:        True if the files have already been copied into
                           the archive, False otherwise
        working_directory: directory to hold files from AD
        filterfunc:        returns True if a filename should be ingested
        make_file_id:      function that turns a file uri/url/path into a file_id
        """
        basecontainer.__init__(self, log, re.sub(r':', '_', vosroot))
        self.vosclient = Client()
        self.filterfunc = filterfunc
        self.make_file_id = make_file_id
        self.archive_name = archive_name
        self.in_archive = in_archive
        
        if self.vosclient.access(vosroot) and self.vosclient.isdir(vosroot):
            self.vosroot = vosroot
        else:
            self.log.console('vosroot does not exist: ' + vosroot,
                             logging.ERROR)

        if os.path.isdir(working_directory):
            self.directory = os.path.abspath(working_directory)
        else:
            self.log.console('not a directory: ' + working_directory,
                             logging.ERROR)
        
        self.dataweb = data_web_client()

        self.vospath = {}
        
        filecount = self.readvos(vosroot)
        if filecount == 0:
            self.log.console('vosroot contains no valid files:' + vosroot)

    def readvos(self, uri):
        """
        Call recursively to read the list of files from a VOspace
        """
        pathlist = [uri + '/' + f for f in self.vosclient.listdir(uri)]
        dirlist = sorted([f for f in pathlist if self.vosclient.isdir(f)])
        filelist = sorted([f for f in pathlist if (self.vosclient.isfile(f) and
                                                   self.filterfunc(f))])
        
        filecount = 0
        for f in filelist:
            file_id = self.make_file_id(f)
            filename = os.path.basename(f)
            if file_id in self.vospath:
                self.log.console('Duplicate file_id: ' + file_id +
                                 ' for ' + self.vospath[file_id] +
                                 ' and ' + f,
                                 logging.ERROR)
            else:
                filecount += 1
                self.vospath[file_id] = f
                self.filedict[file_id] = os.path.join(self.directory,
                                                      filename)
        
        for d in dirlist:
            filecount += self.readvos(d)
        
        return filecount
            
    def get(self, file_id):
        """
        Fetch a file from ad into the working directory

        Arguments:
        file_id : The file_id to extract from the archive
        """
        if file_id not in self.filedict:
            self.log.console('requesting bad file_id: ' + file_id +
                             ' from ' +repr(self.file_id_list()),
                             logging.ERROR)

        # This fetches only the header from the primary HDU, which
        # should result in significant performance improvements
        if self.in_archive:
            filepath = self.dataweb.get(self.archive_name, 
                                        file_id,
                                        params=data_web_client.PrimaryHEADER)
            if not filepath:
                self.log.console('could not get ' + file_id + ' from ' + 
                                 self.archive_name,
                                 logging.ERROR)
            self.filedict[file_id] = filepath
        else:
            filepath = self.filedict[file_id]
            filesize = self.vosclient.copy(self.vospath[file_id], filepath)
            if not filesize:
                self.log.console('could not get ' + file_id + ' from ' + 
                                 filepath,
                                 logging.ERROR)
        
        return filepath

    def cleanup(self, file_id):
        """
        Clean up deletes the file from the working directory

        Arguments:
        file_id : file_id of the file to delete
        """
        os.remove(self.filedict[file_id])
