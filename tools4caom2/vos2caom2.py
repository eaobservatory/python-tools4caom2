#!/usr/bin/env python2.7

__author__ = "Russell O. Redman"

import argparse
import commands
from ConfigParser import SafeConfigParser
from contextlib import contextmanager
from collections import OrderedDict
import datetime
import logging
import os
import os.path
import pyfits
import re
import shutil
import subprocess
from subprocess import CalledProcessError
import sys
import traceback

from vos.vos import Client

from tools4caom2.data_web_client import data_web_client
from tools4caom2.ingest2caom2 import ingest2caom2
from tools4caom2.logger import logger
from tools4caom2.utdate_string import utdate_string

from tools4caom2 import __version__ as tools4caom2version

__doc__ = """
The vos2caom2 is a specialization of ingest2caom2 that expects to find 
its list of input files in a VOspace.  It organizes the testing and copying 
of the files from the VOspace to the CADC data web service, then ingests
the files into CAOM-2.   

The process of copying files  VOS -> CADC is in some respects modeled after the 
CADC e-transfer service.  In particular, it implements several of the checks that 
would normally be done during e-transfer, such as rejecting zero-length files,
and verifying that names match those required  for the archive.  A couple of 
additional checks have been added, including testing fits files with fitsverify
and verifying whether the file is already present in the archive.   

As with ingest2caom2, most of the ingestion functionality is missing from this 
module, and the intended usage is for an archive-specific subclass to be derived
from vos2caom2 with the details filled in.  

This class specifically deprecates ingestion from directories on disk and tar 
files.  Although it can, in principle, be used to re-ingest sets of files already 
present in AD, They would first have to be copied into a VOspace and sorted into
appropriate folders.

This module provides a command-line interface that should be sufficiently 
general to handle most archival needs.  The interface allows the program to 
be run in three different modes:
1) check - verify that the files are ready for ingestion
2) persist - copy files from VOspace to AD using the CADC data web service
3) ingest - ingest the files from AD
The mode must be selected explicitly using a command line argument to prevent 
unintended ingestion, or failure to ingest, through the omission of a required 
argument.  Ingestion is kept independent from the previous two modes because it
is the most time consuming step and might benefit from multiprocessing.

In check mode, a combination of stubs and generic code are provided to:
- read all the files from a VOspace,
- filter out any that are not candidates for ingestion,
- apply basic validity tests:
  - size is non-zero,
  - files match naming rules, with functionality like the CADC namecheck,
  - checks that the file_id is not already in use,
  - FITS files pass fitsverify without warnings or errors,
  - ingestion code is run in check mode to identify problems,
- a report is generated that can be passed back to the data providers.

Persist mode is the same except that after files have been checked,
- the files will be copied into CADC storage using the CADC data web service,
- the ingestion code will be invoked to ingest the metadata into CAOM-2.

The vos2caom2 module is intended to be used at sites remote from the CADC, so
uses only generic methods that should work ob=ver the internet to access and 
store files. Access to the VOspace uses the CADC-supplied vos module.  Storage 
of files in the CADC archives uses the data_web_client supplied as part of the
python-tools4caom2 package, which gets, puts, gets info about, and deletes files
through the CADC data web service. 
"""
#************************************************************************
#* Utility routines
#************************************************************************
def make_file_id(filepath):
    """
    An archive-specific routine to convert a file basename (without the
    directory path) to the corressponding file_id used to identify the
    file in CADC storage.  The default routine provided here picks out the
    basename from the path, which can therefore be a path to a file on
    disk, a VOspace urL, or a vos uri, then strips off the extension and 
    forces the name into lower case.

    Arguments:
    filepath: path to the file
    
    Returns:
    file_id: string used to identify the file in storage
    This is a static method taking exactly one argument.
    """
    return os.path.splitext(
                os.path.basename(filepath))[0].lower()

class vosfilter(object):
    """
    A generic filter class that can be customized for different purposes.
    A vosfilter is a callable class that examines filenames for problems
    and records the bad files in a dictionary of errors classified by 
    the type of error.
    """
    def __init__(self, errortype, errors):
        """
        A vosfilter is a callable object that examines filenames for possible
        problems.  If problems are found, the offending filename is recorded 
        in the dictionary errors.  Since self.errors is a reference 
        to the dictionary supplied by the caller, recording the error will 
        update the original dictionary, which can subsequently be examined for 
        accumulated problems.
        """
        self.errortype = errortype
        self.errors = errors
    
    def record(self, filename):
        """
        Record this file as having a problem.  
        """
        if filename not in self.errors:
            self.errors[filename] = []
        if self.errortype not in self.errors[filename]:
            self.errors[filename].append(self.errortype)
        
    def __call__(self, filename):
        """
        A vosfilter instance is a callable object taking exactly one argument.
        Override this method for each error type.
        
        Arguments:
        filename: the file name to be tested
        
        Retuens:
        True if the filename is acceptable, False otherwise
        
        Note that False does not flag an error, only indicating that the file 
        should be filtered out of the list of acceptable files.  The file 
        should be recorded in self.errors if there is an actual error condition.
        """
        pass
    
class namecheck(vosfilter):
    def __init__(self, 
                 errors,
                 fileid_regex=[], 
                 extensions=['.fits', '.fit']):
        """
        Configure the filter with a list of regex expressions and the 
        extensions of files that are candidates for ingestion.
        
        Arguments:
        errors: (required) a dictionary created in the calling routine
                that will record filenames with namecheck problems
        fileid_regex: a list of regex strings to match against fileid's
        extensions: a list of extensions of files that are candidates for 
                    ingestion, defaulting to common fits extensions
        """
        vosfilter.__init__('namecheck', errors)
        self.fileid_regex = []
        for regex in fileid_regex:
            self.fileid_regex.append(re.compile(regex))
        self.extensions = extensions
    
    def __call__(self, filename):
        """
        Arguments:
        filename : the file name to check for validity
    
        Returns True if filename has the correct extension and passes namecheck, 
        False otherwise
        """
        fileid, ext = os.path.splitext(os.path.basename(filename))
        ok = False
        if ext in self.extensions:
            # Only files that are candidates for ingestion are tested by 
            # namecheck
            for regex in self.fileid_regex:
                if regex.match(fileid):
                    ok = True
                    break
            else:
                self.record(filename)
        return ok

class zerolength(vosfilter):
    def __init__(self, 
                 errors):
        """
        Check for files that have no content.
        
        Arguments:
        errors: (required) a dictionary created in the calling routine
                that will record filenames with zerolength problems
        """
        vosfilter.__init__('zerolength', errors)
        self.client = Client()
    
    def __call__(self, filename):
        """
        Arguments:
        filename : uri of a file in a VOspace
        
        Returns True if the file has a non-zero length, False otherwise
        """
        ok = False
        length = self.client.getNode(filename).getPropValue('length')
        if length:
            ok = True
        else:
            self.record(filename)
        return ok

class fitsverify(vosfilter):
    def __init__(self, 
                 errors):
        """
        Check that FITS files generate no errors or warnings with fitsverify
        
        Arguments:
        errors: (required) a dictionary created in the calling routine
                that will record filenames with zerolength problems
        """
        vosfilter.__init__('zerolength', errors)
        # If you neeed fitsverify and it does not exist, that is an 
        # immediately fatal error
        self.fitsverify = str(subprocess.check_output(
                                ['which', 'fitsverify']))
    
    def __call__(self, filename):
        """
        Arguments:
        filename : uri of a file in a VOspace
        
        Returns True if the file has a non-zero length, False otherwise
        """
        ok = False
        error_count = 1
        warning_count = 0
        try:
            output = str(subprocess.check_output(['fitsverify', filename]))
            error_count = re.sub(r'.*?\s(\d+) error.*', r'\1', output)
            warning_count = re.sub(r'.*?\s(\d+) warning.*', r'\1', output)
        except Exception as e:
            # absorb all exceptions, but such files are recorded as 
            # causing errors
            pass
        
        if error_count or warning_count:
            self.record(filename)
        else:
            ok = True
        return ok

class nameconflict(vosfilter):
    def __init__(self, 
                 errors,
                 archive,
                 make_file_id):
        """
        Check for files that are already in the CADC archive.  Call this filter
        in check mode before the files have been pushed into the archive,
        and only when the files are expected to be new entries.  
        
        Arguments:
        errors: (required) a dictionary created in the calling routine
                that will record filenames with nameconflict problems
        archive: the CADC archive to check
        make_file_id: a function that turns filenames (possibly with paths)
                      into archive-specific file_id strings
        """
        vosfilter.__init__('name conflict', errors)
        self.archive = archive
        self.dataclient = data_web_client()
        self.make_file_id = make_file_id
    
    def __call__(self, filename):
        """
        Arguments:
        filename : uri of a file in a VOspace

        Return True if the fileid is not present in the archive, False if
        there is a name conflict with an existing file.
        """
        ok = False
        fileid = self.make_file_id(filename)
        if self.dataclient.info(self.archive, fileid):
            self.record(filename)
        else:
            ok = True
        return ok

class notinarchive(vosfilter):
    def __init__(self, 
                 errors,
                 archive,
                 make_file_id):
        """
        Check for files that are not in the CADC archive.  Call this filter
        after the files have already been pushed into the archive.  
        
        Arguments:
        errors: (required) a dictionary created in the calling routine
                that will record filenames with notinarchive problems
        archive: the CADC archive to check
        make_file_id: a function that turns filenames (possibly with paths)
                      into archive-specific file_id strings
        """
        vosfilter.__init__('not in archive', errors)
        self.archive = archive
        self.dataclient = data_web_client()
        self.make_file_id = make_file_id
    
    def __call__(self, filename):
        """
        Arguments:
        filename : uri of a file in a VOspace
        
        Return True if the fileid is present in the archive as it should be, 
        False if it is missing.
        """
        ok = False
        fileid = self.make_file_id(filename)
        if self.dataclient.info(self.archive, fileid):
            ok = True
        else:
            self.record(filename)
        return ok

class missingmandatory(vosfilter):
    def __init__(self, 
                 errors,
                 key):
        """
        Call this filter to check for mandatory headers
        
        Arguments:
        errors: (required) a dictionary created in the calling routine
                that will record filenames with namecheck problems
        fileid_regex: a list of regex strings to match against fileid's
        extensions: a list of extensions of files that are candidates for 
                    ingestion, defaulting to common fits extensions
        """
        vosfilter.__init__('missing mandatory header ' + key, errors)
        self.key = key
    
    def __call__(self, filename, header):
        """
        Arguments:
        filename : uri of a file in a VOspace
        header: FITS header from the primary HDU
        
        Return True if the mandatory key is found in header, False otherwise
        """
        ok = False
        if self.key in header and header[self.key] != pyfits.card.UNDEFINED:
            ok = True
        else:
            self.record(filename)
        return ok


class vos2cadc(ingest2caom2):
    """
    Base class to copy and ingest files from a VOspace into a CADC archive
    """

    def __init__(self):
        """
        Initialize the vos2cadc structure, especially the attributes
        storing default values for command line switches.

        Arguments:
        <none>

        It is normally necessary to override __init__ in a derived class,
        supplying archive-specific values for some of the fields, e.g.
            def __init__(self):
                ingest2caom2.__init__(self)
                self.archive  = <myarchive>
        """
        ingest2caom2.__init__(self)
        self.userconfigpath = '~/.tools4caom2/tools4caom2.config'

        # -------------------------------------------
        # placeholders for command line switch values
        # -------------------------------------------
        # Command line interface for the ArgumentParser and arguments
        self.ap = argparse.ArgumentParser('ingest2caom2')
        self.args = None
        
        # routine to convert filepaths into file_ids
        self.make_file_id = make_file_id

        # temporary disk space for working files
        self.workdir = None

        # log handling
        self.logdir = None
        self.logfile = None
        self.loglevel = logging.INFO
        self.debug = False
        self.log = None

        # The filterfunc is a name checking function that returns True if
        # a filename is valid for ingestion and False otherwise.
        # The signature of filterfunc is filterfunc(filename), i.e. it
        # operates on filenames rather than file_id's and can use the
        # file extension to help determine if the file name is valid.
        # By default, ingest only FITS files.
        self.filterfunc = fitsfilter

        # The metadata dictionary - fundamental structure for the entire class
        # For the detailed structure of metadict, see the help text for
        # fillMetadictFromFile()
        self.metadict = OrderedDict()

    #************************************************************************
    # Run the program
    #************************************************************************
    def run(self):
        """
        Generic method to run the checks or persistence

        Arguments:
        <none>
        """
        # metadict is the fundamental structure in the program, sorting
        # files by observation, plane and file, and holding all the relevant
        # metadata in a set of nested dictionaries.
        self.metadict = {}
        self.defineCommandLineSwitches()
        self.processCommandLineSwitches()

        with logger(self.logfile,
                    loglevel=self.loglevel).record() as self.log:
            self.logCommandLineSwitches()
            # Read list of files from VOspace and do things
            
            # if no errors, declare we are DONR
            self.log.console('DONE')
