#!/usr/bin/env python2.7

__author__ = "Russell O. Redman"

import argparse
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

from vos.vos import Client as vosclient

from tools4caom2.data_web_client import data_web_client
from tools4caom2.logger import logger
from tools4caom2.__version__ import version as tools4caom2version

#*******************************************************************************
# A set of classes that allows error and warning messages to be recorded
# without triggering an immediate exit.  Errors and warnings are logged as ERROR 
# and WARNING messages, but all errors are trapped and a final call to sys.exit
# made if any errors are reported.
#*******************************************************************************
class delayed_error_warning(object):
    """
    A delayed_error_warning examines files for problems and records bad files 
    in dictionaries of errors and warnings.
    """
    def __init__(self, 
                 log,
                 outdir,
                 archive,
                 fileid_regex_dict,
                 make_file_id,
                 debug=False):
        """
        A delayed_error_warning is a class that examines files for possible
        problems.  If problems are found, the offending filename is recorded 
        in the dictionary errors.
        
        Arguments:
        log               : a tools4caom2.logger object
        outdir            : local directory for temporary files
        fileid_regex_dict : dictionary keyed on extension containing a list
                            of compiled regex objects matching valid file_ids
        make_file_id      : function returning a file_id from a filename
        """
        self.errors = {}
        self.warnings = {}
        self.log = log
        self.outdir = outdir
        self.archive = archive
        self.vosclient = vosclient()
        self.fileid_regex_dict = fileid_regex_dict
        self.data_web_client = data_web_client(outdir, log)
        # Has fitsverify been installed on the PATH?
        self.fitsverifypath = \
            str(subprocess.check_output(['which', 'fitsverify'])).strip()
        self.make_file_id = make_file_id
        self.debug = debug
        
    @contextmanager
    def gather(self):
        """
        When done, generate the report
        """
        try:
            yield self
        finally:
            self.report()
    
    def error_count(self):
        """
        Return number of files for which errors were reported
        """
        return len(self.errors)
    
    def error(self, filename, errormsg):
        """
        Record this file as having an error
        
        Arguments:
        filename  : file name to examine 
        errormsg  : error message to report
        """
        self.log.file('delayed_error_warning: error for filename' + filename)
        if filename not in self.errors:
            self.errors[filename] = []
        if errormsg not in self.errors[filename]:
            self.errors[filename].append(errormsg)
    
    def warning(self, filename, warningmsg):
        """
        Record this file as having a warning  
        
        Arguments:
        filename  : file name to examine 
        warningmsg  : warning message to report
        """
        self.log.file('delayed_error_warning: warning for filename' + filename)
        if filename not in self.warnings:
            self.warnings[filename] = []
        if warningmsg not in self.warnings[filename]:
            self.warnings[filename].append(warningmsg)
    
    def report(self):
        """
        Report accumulated error and warning messages.  If there were errors,
        exit after the report is complete.
        
        Arguments:
        <none>
        """
        filelist = []
        if self.errors:
            filelist.extend(self.errors.keys())
        if self.warnings:
            filelist.extend(self.warnings.keys())
        if filelist:
            self.log.console('ERRORS and WARNINGS')
            filelist = sorted(filelist)
            for filename in filelist:
                self.log.console('')
                self.log.console(filename)
                if filename in self.errors:
                    for errormsg in self.errors[filename]:
                        self.log.console(errormsg, 
                                         logging.ERROR, 
                                         raise_exception=False)
                if filename in self.warnings:
                    for warningmsg in self.warnings[filename]:
                        self.log.console(warningmsg, logging.WARN)
    
    #************************************************************************
    # The following methods test for individual conditions.  Each test will 
    # return True if the file is acceptable for ingestion.
    #
    # To test more/different conditions, derive a new class from 
    # delayed_error_warning and add the new tests to the derived class.
    #
    # Arbitrary messages can also be added by calling error or warning directly,
    # leaving the logic of the test external to the class.
    #***********************************************************************
    def sizecheck(self, filename):
        """
        Arguments:
        filename : filesystem path or VOspace uri of a file
        
        Returns True if the file has a non-zero length, False otherwise
        """
        self.log.file('delayed_error_warning: sizecheck for filename' + filename)
        ok = False
        length = 0
        if re.match(r'vos:', filename):
            length = int(self.vosclient.getNode(filename).getInfo()['size'])
        elif os.path.isfile(filename):
            length = os.path.getsize(filename)
        
        if length:
            ok = True
        else:
            self.error(filename, 'file has length = 0')
        return ok

    def namecheck(self, filename, report=True):
        """
        Returns True if the filename matches one of a list of acceptable
        regex patterns keyed by the extension, False otherwise
        
        Arguments:
        filename : filesystem path or VOspace uri of a file
        fileid_regex_dict : a dictionary keyed by file extension that
                            contains lists of regex to match against fileid's
        report : True is failing namecheck is a reportable error, False if
                 namecheck is being used to fileter a list of files.
        
        Arguments:
        """
        self.log.file('delayed_error_warning: namecheck for filename' + filename)
        ext = os.path.splitext(filename)[1].lower()
        file_id = self.make_file_id(filename)
        ok = False
        if ext in self.fileid_regex_dict:
            # Only files that are candidates for ingestion are tested by 
            # namecheck
            for regex in self.fileid_regex_dict[ext]:
                if regex.match(file_id):
                    ok = True
                    break
            else:
                if report:
                    self.error(filename, 'namecheck: should match (one of) ' +
                               repr([r.pattern for r in fileid_regex_dict[ext]]))
                       
        return ok

    def in_archive(self, filename, severity_dict):
        """
        Check whether a file is or is not present in the archive
        
        Arguments:
        filename      : filesystem path or VOspace uri of a file
        severity_dict : dictionary telling whether it is an error, warning or 
                        acceptable for the file to be present (True) or absent 
                        (False) in the archive.
        """
        self.log.file('delayed_error_warning: in_archive for filename' + 
                      filename)
        ok = False
        file_id = self.make_file_id(filename)
        if self.data_web_client.info(self.archive, file_id):
            if severity_dict[True] == 'error':
                self.error(filename, 
                           'name conflict with existing file in ' + self.archive)
            elif severity_dict[True] == 'warning':
                ok = True
                self.warning(filename, 
                             'existing file has this name in ' + self.archive)
            else:
                ok = True
        else:
            if severity_dict[False] == 'error':
                self.error(filename, 
                           'expected file not found in ' + self.archive)
            elif severity_dict[False] == 'warning':
                ok = True
                self.warning(filename, 
                             'file is not present in ' + self.archive)
            else:
                ok = True
        return ok

    def fitsverify(self, filename):
        """
        Check that fitsverify reports no  errors for this file
        
        Arguments:
        filename : filesystem path to a file
        
        Do not run this test on files in VOspace because it is necessary to
        copy them to the local disk before it is possible to run fitsverify.
        
        If fitsverify is not installed, the test will pass by default.
        """
        self.log.file('delayed_error_warning: fitsverify for filename' + 
                      filename)
        ok = False
        if self.fitsverifypath:
            error_count = '1'
            output = ''
            try:
                # fitsverify will return a non-zero exit code if there were
                # errors or warnings, but can fail for other reasons as well
                output = subprocess.check_output([self.fitsverifypath,
                                                  '-q',
                                                  filename])
            except subprocess.CalledProcessError as e:
                # absorb all exceptions, but such files are recorded as 
                # causing errors
                output = str(e.output)
            except:
                output = type(e) + ': 1 errors'
            
            if re.search(r'\s*verification OK', output):
                error_count = '0'
            else:
                error_count = re.sub(r'.*?\s(\d+) errors.*', r'\1', output)
            
            if int(error_count):
                self.error(filename, 
                           'fitsverify reported ' + error_count + ' errors')
            else:
                ok = True
        else:
            # if fitsverify is not installed, return True
            ok = True
            
        return ok

    def expect_keyword(self, filename, key, header, mandatory=False):
        """
        Return True if the mandatory key is defined in header, False otherwise

        Arguments:
        filename : filesystem path to a file
        header   : FITS header from the primary HDU
        key      : mandatory keyword
        """
        self.log.file('delayed_error_warning: expect_keyword for filename' + 
                      filename)
        ok = False
        if key in header and header[key] != pyfits.card.UNDEFINED:
            ok = True
        else:
            qualifier = 'expected'
            if mandatory:
                qualifier = 'mandatory'
            self.error(filename, 
                       qualifier + ' keyword "' + key + 
                       '" is missing or undefined')
        return ok

    def restricted_value(self, filename, key, header, value_list):
        """
        Return True if the header[key] in value_list, False otherwise

        Arguments:
        filename   : filesystem path to a file
        key        : keyword whose value must be in the value_list
        header     : FITS header from the primary HDU
        value_list : list of acceptable values
        """
        self.log.file('delayed_error_warning: restricted_value for filename' + 
                      filename)
        ok = False
        if key in header and header[key] != pyfits.card.UNDEFINED:
            if header[key] in value_list:
                ok = True
            else:
                self.error(filename, 
                           'header[' + key + '] = "' + header[key] + 
                           '" must be in' + repr(value_list))
        else:
            self.error(filename,
                       'keyword "' + key + '" with a restricted set of values '
                       'is missing or undefined')
            
        return ok