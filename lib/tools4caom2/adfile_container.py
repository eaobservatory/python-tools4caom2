__author__ = "Russell O. Redman"

import commands
import logging
import os
import os.path
import re
import time

from tools4caom2 import __version__
from tools4caom2.basecontainer import basecontainer
from tools4caom2.data_web_client import data_web_client
from tools4caom2.error import CAOMError

__doc__ = """
The adfile_container class reads from a text file a list of AD URIs that
reference the files to ingest.

Version: """ + __version__.version

logger = logging.getLogger(__name__)


class adfile_container(basecontainer):
    def __init__(self,
                 dataweb,
                 adfile,
                 working_directory,
                 filterfunc):
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
        dataweb:           a tools4caom2.data_web_client object
        adfile:            path to file containing a list of adURI's
        working_directory: directory to hold files from AD
        filterfunc:        returns True if a filename should be ingested
        """
        basecontainer.__init__(self, os.path.basename(adfile))

        if not os.path.exists(adfile):
            raise CAOMError('ad_file does not exist: ' + adfile)

        if os.path.isdir(working_directory):
            self.directory = os.path.abspath(working_directory)
        else:
            raise CAOMError('not a directory: ' + working_directory)

        self.dataweb = dataweb

        self.archive = {}

        with open(adfile, 'r') as ADF:
            filecount = 0
            for line in ADF:
                match = re.match(r'^[|\s]*ad:([A-Z]+)/([a-zA-Z0-9.\-_]+)',
                                 line)
                if match:
                    (archive, file_id) = match.group(1, 2)
                    headers = self.dataweb.info(archive, file_id)
                    if headers and 'content-disposition' in headers:
                        m = re.match(r'^.*?filename=(.+)$',
                                     headers['content-disposition'])
                        if m:
                            adfilename = m.group(1)
                            # use the adfilename to filter, but do not record
                            # the name on disk, which will change when the
                            # cutout for the primary header is done in get
                            if not filterfunc or filterfunc(adfilename):
                                self.filedict[file_id] = ''
                                self.archive[file_id] = archive
                                filecount += 1
                        else:
                            raise CAOMError('data web service cannot find ' +
                                            archive + '/' + file_id)

            if filecount == 0:
                raise CAOMError('adfile ' + adfile +
                                ' contains no valid ad URIs')

    def get(self, file_id):
        """
        Fetch a file from ad into the working directory

        Arguments:
        file_id : The file_id to extract from the archive
        """
        if file_id not in self.archive:
            raise CAOMError('requesting bad file_id: ' + file_id +
                            ' from ' + repr(self.file_id_list()))

        # This fetches only the header from the primary HDU, which
        # should result in significant performance improvements
        filepath = self.dataweb.get(self.archive[file_id],
                                    file_id,
                                    params=data_web_client.PrimaryHEADER)
        if not filepath:
            raise CAOMError('could not get ' + file_id + ' from ' +
                            self.archive[file_id])

        self.filedict[file_id] = filepath
        return filepath

    def cleanup(self, file_id):
        """
        Clean up deletes the file from the working directory

        Arguments:
        file_id : file_id of the file to delete
        """
        os.remove(self.filedict[file_id])
