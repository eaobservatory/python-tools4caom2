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

try:
    import Sybase
    sybase_defined = True
except:
    sybase_defined = False

from vos.vos import Client

from tools4caom2.database import connection
from tools4caom2.data_web_client import data_web_client
from tools4caom2.delayed_error_warning import delayed_error_warning as dew
from tools4caom2.filelist_container import filelist_container
from tools4caom2.logger import logger
from tools4caom2.tapclient import tapclient
from tools4caom2.utdate_string import utdate_string
from tools4caom2.vos_container import vos_container

from tools4caom2.__version__ import version as tools4caom2version

__doc__ = """
The vos2caom2 has been cloned from ingest2caom2 and customized to find 
its lists of input files in a VOspace.  It organizes the testing and copying 
of the files into the JSA, then ingests the files into CAOM-2.   

The process of copying files  VOS -> CADC is in some respects modeled after the 
CADC e-transfer service.  In particular, it implements several of the checks that 
would normally be done during e-transfer, such as rejecting zero-length files,
and verifying that names match those required  for the archive.  Other checks 
include testing fits files with fitsverify and verifying whether the file is 
present in the archive (sometimes forbidden, sometimes mandatory).   

As with ingest2caom2, most of the ingestion logic is missing from this 
module, and it is intended that an archive-specific subclass will be derived
from vos2caom2 to supply the custom logic.  

This class specifically deprecates ingestion from directories on disk and tar 
files.  Although it can, in principle, be used to re-ingest sets of files already 
present in AD, they would first have to be copied into a VOspace and sorted into
appropriate folders.

This module provides a command-line interface that should be sufficiently 
general to handle most archival needs.  The interface allows the program to 
be run in several different modes:
1) new - check metadata and report as errors any pre-existing observations, 
         planes and files.
2) check - check metadata without reporting pre-existing observations, planes 
           and files
3) replace - check metadata and report as errors any observations, planes and 
             files that do not already exist.
4) store - copy files from VOspace to AD using the CADC data web service
5) ingest - ingest the files from AD.
The mode must be selected explicitly using a command line argument to prevent 
unintended ingestion, or failure to ingest, through the omission of a required 
argument.  Ingestion is kept independent from the previous modes because it
is the most time consuming step and might benefit from multiprocessing.

In new/check/replace mode, stubs and generic code are provided to:
- read all the files from a VOspace,
- filter out any that are not candidates for ingestion,
- apply basic validity tests:
  - size is non-zero,
  - files match naming rules, with functionality like the CADC namecheck,
  - checks that the file_id is not already in use,
  - FITS files pass fitsverify without warnings or errors,
- a report is generated that can be passed back to the data providers.

Store mode is the same except that after files have been checked the files will 
be copied into CADC storage using the CADC data web service.

Ingest mode will skip most of the validity tests and ingest the metadata into 
CAOM-2 working from the copies of the files in storage.

The vos2caom2 module is intended to be used at sites remote from the CADC, so
uses only generic methods that should work over the internet to access and 
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
    return os.path.splitext(os.path.basename(filepath))[0].lower()

#*******************************************************************************
# Base class for ingestions from VOspace
#*******************************************************************************
class vos2caom2(object):
    """
    Base class to copy and ingest files from a VOspace into a CADC archive
    """

    def __init__(self):
        """
        Initialize the vos2cadc structure, especially the attributes
        storing default values for command line arguments.

        Arguments:
        <none>

        It is normally necessary to override __init__ in a derived class,
        supplying archive-specific values for some of the fields, e.g.
            def __init__(self):
                vos2caom2.__init__(self)
                self.archive  = <myarchive>
        """
        # config object optionally contains a user configuration object
        # this can be left undefined at the CADC, but is needed at other sites
        self.userconfig = SafeConfigParser()
        self.userconfigpath = '~/.tools4caom2/tools4caom2.config'

        # -------------------------------------------
        # placeholders for command line switch values
        # -------------------------------------------
        # Command line interface for the ArgumentParser and arguments
        # Command line options
        self.progname = os.path.basename(os.path.splitext(sys.path[0])[0])
        self.exedir = os.path.abspath(os.path.dirname(sys.argv[0]))
        self.configpath = os.path.abspath(self.exedir + '/../config')

        # Argument parser
        self.ap = None
        self.args = None
        
        # Database defaults, filled from userconfig 
        self.sybase_defined = sybase_defined
        self.archive = None
        self.stream = None
        self.schema = None
        self.collection_choices = ['SANDBOX']
        
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

        # Ingestion parameters and structures
        self.prefix = ''
        self.major = ''
        self.minor = []
        self.replace = []
        self.store = False
        self.ingest = False
        self.local = False
        
        # Archive-specific fits2caom2 config and default file paths
        self.config = None
        self.default = None
        
        # Current vos container
        self.vosclient = Client()
        self.vos = None
        self.local = None
        # default fileid_regex_dict will pass all .fits files
        self.fileid_regex_dict = None
        
        # Working structures
        # The metadata dictionary - fundamental structure for the entire class
        # For the detailed structure of metadict, see the help text for
        # fillMetadictFromFile()
        self.collection = None
        self.observationID = None
        self.productID = None
        self.plane_dict = OrderedDict()
        self.fitsuri_dict = OrderedDict()
        self.metadict = OrderedDict()
        self.data_storage = []
        self.preview_storage = []
        
        # local sets to be accumulated in a plane
        self.memberset = set([])
        self.inputset = set([])

        # list of containers for input files
        self.containerlist = []

        # Delayed errors and warnings discovered in files
        self.dew = None
        
        # TAP client
        self.tap = None

    #************************************************************************
    # Define the standardcommand line interface.
    # Be sure to maintain consistency amongst defineCommandLineSwitches,
    # processCommandLineSwitches, and logCommandLineSwitches.
    #************************************************************************
    def defineCommandLineSwitches(self):
        """
        Generic routine to build the standard list of command line arguments.
        This routine has been split off from processing and logging to allow
        additional arguments to be defined for derived classes.
        
        Subclasses for specific archive can override this method to add new
        arguments, but should first call 
           self.vos2caom2.defineCommandLineSwitches()
        to ensure that the standard arguments are always defined.

        Arguments:
        <none>

        # user config arguments
        --userconfig : path to user configuration file
        --proxy      : path to CADC proxy certificate
        
        # ingestion arguments
        --prefix     : (required) prefix for FITS files to be stored/ingested
        --major      : (required) major release directory
        --minor      : (optional) comma-separated list of directories relative to
                       the major release directory.
        --replace    : (optional) comma-separated list of major release 
                       directories whose contents may be replaced by the new 
                       major release.  All major releases share the same minor 
                       release directory structure.
        --store      : (optional) store files in AD (requires CADC authorization)
        --ingest     : (optional) ingest new files (requires CADC authorization)

        # fits2caom2 arguments
        --collection : (required) collection to use for ingestion
        --config     : (optional) path to fits2caom2 config file
        --default    : (optional) path to fits2caom2 default file

        # File and directory options
        --workdir    : (optional) working directory (default = cwd)

        # debugging options
        --log        : (optional) name of the log file
        --logdir     : directory to hold log files
        --debug      : (optional) log all messages and retain temporary files
                       on error
        --test       : (optional) simulate operation of fits2caom2

        Log files are always opened in append.  Be sure to delete
        existing log files if it is important to have a clean record of the
        current ingestion.
        """

        # Optional user configuration
        if self.userconfigpath:
            self.ap.add_argument('--userconfig',
                default=self.userconfigpath,
                help='Optional user configuration file '
                     '(default=' + self.userconfigpath + ')')
        
        self.ap.add_argument('--proxy',
            default='~/.ssl/cadcproxy.pem',
            help='path to CADC proxy')

        # Ingestion modes
        self.ap.add_argument('--prefix',
                             help='file name prefix that identifies FITS files '
                                  'to be ingested')
        self.ap.add_argument('--major',
                             required=True,
                             help='path to major release directory '
                                  'containing files to store or ingest')
        self.ap.add_argument('--minor',
                             help='comma-separated list of minor release '
                                  'directories relative to the major release '
                                  'directory')
        self.ap.add_argument('--replace',
                             help='comma-separated list of major release '
                                  'directories containing files that are '
                                  'being replaced')
        self.ap.add_argument('--store',
                             action='store_true',
                             help='store in AD files that are ready for '
                                  'ingestion if there are no errors')
        self.ap.add_argument('--ingest',
                             action='store_true',
                             help='ingest from AD files that are ready for '
                                  'ingestion if there are no errors')

        # Basic fits2caom2 options
        # Optionally, specify explicit paths to the config and default files
        self.ap.add_argument('--collection',
            required=True,
            choices=self.collection_choices,
            help='collection to use for ingestion')
        self.ap.add_argument('--config',
            help='(optional) path to fits2caom2 config file')
        self.ap.add_argument('--default',
            help='(optional) path to fits2caom2 default file')

        # Big jobs require extra memory
        self.ap.add_argument('--big',
            action='store_true',
            help='(optional) request extra heap space and RAM')
        
        # output directory
        self.ap.add_argument('--workdir',
            help='output directory, (default = current directory')

        # debugging options
        self.ap.add_argument('--logdir',
            help='(optional) directory to hold log file')
        self.ap.add_argument('--log',
            help='(optional) name of the log file')
        self.ap.add_argument('--test',
            action='store_true',
            help='(optional) simulate operation of fits2caom2')
        self.ap.add_argument('--debug',
            action='store_true',
            help='(optional) show all messages, pass --debug to fits2caom2,'
            ' and retain all xml and override files')
    
    def processCommandLineSwitches(self):
        """
        Generic routine to process the command line arguments
        and create workdir if necessary.  This will check the values of the
        standard arguments defined in defineCommandLineSwitches and will
        leave the additional arguments in self.args.
        
        Arguments:
        <None>
        
        Returns:
        The set of command line arguments is stored in self.args and the
        default arguments are interpreted and stored into individual attributes.
        """
        # If the user configuration file exists, read it.
        if 'userconfig' in self.args:
            self.userconfigpath = os.path.abspath(
                                    os.path.expanduser(
                                        os.path.expandvars(
                                            self.args.userconfig)))
        if self.userconfigpath and os.path.isfile(self.userconfigpath):
            with open(self.userconfigpath) as UC:
                self.userconfig.readfp(UC)
        
        self.proxy = os.path.abspath(
                        os.path.expandvars(
                            os.path.expanduser(self.args.proxy)))
        
        if self.args.prefix:
            self.prefix = self.args.prefix
            file_id_regex = re.compile(self.prefix + r'.*')
            self.fileid_regex_dict = {'.fits': [file_id_regex],
                                      '.fit': [file_id_regex]}
        else:
            self.fileid_regex_dict = {'.fits': [re.compile(r'.*')],
                                      '.fit': [re.compile(r'.*')]}

        # Save the values in self
        # A value on the command line overrides a default set in code.
        # Options with defaults are always defined by the command line.
        # It is not necessary to check for their existance.
        if self.args.big:
            self.big = self.args.big
        
        if self.args.config:
            self.config = os.path.abspath(
                                os.path.expandvars(
                                    os.path.expanduser(self.args.config)))
        if self.args.default:
            self.default = os.path.abspath(
                                os.path.expandvars(
                                    os.path.expanduser(self.args.default)))

        if self.args.workdir:
            self.workdir = os.path.abspath(
                             os.path.expandvars(
                                 os.path.expanduser(self.args.workdir)))
        else:
            self.workdir = os.getcwd()
        
        self.local = self.args.local
        
        if self.args.logdir:
            self.logdir = os.path.abspath(
                            os.path.expandvars(
                                os.path.expanduser(self.args.logdir)))
        else:
            self.logdir = os.getcwd()

        self.test = self.args.test

        if self.args.debug:
            self.loglevel = logging.DEBUG
            self.debug = True

        logbase = re.sub(r'[^a-zA-Z0-9]', r'_', 
                         os.path.splitext(
                             os.path.basename(
                                 self.args.major)))
        logbase += '_'

        # log file name
        # If the log file already exists, do not delete it on successful exit.
        # Otherwise, the default behaviour will be to delete a log file that
        # is created for this program on successful exit.
        if self.args.log:
            if os.path.dirname(self.args.log) == '':
                self.logfile = os.path.join(self.logdir, self.args.log)
            else:
                self.logfile = os.path.abspath(self.args.log)
        
        if not self.logfile:
            self.logfile = os.path.join(self.logdir,
                                        logbase + utdate_string() + 
                                        '.log')
        
        # create workdir if it does not already exist
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

        # Parse ingestion options 
        self.major = self.args.major
        if os.path.isdir(self.major):
            self.local = True
            self.collection = 'SANDBOX'
            self.log.console('When --major is a directory on disk, collection '
                             'will be set to SANDBOX')
        elif (self.vosclient.access(self.major) 
              and self.vosclient.isdir(self.major)):
            self.local = False
        else:
            self.log.console('major does not exist: ' + self.major,
                             logging.ERROR)
        
        if self.args.minor:
            self.minor = re.sub(r'/+', '/', 
                                self.args.minor.strip(' \t\n')).split(',')
        
        if self.args.replace:
            self.replace = re.sub(r'/+', '/', 
                                self.args.replace.strip(' \t\n')).split(',')
        
        if self.args.store:
            self.store = self.args.store
        
        if self.args.ingest:
            self.ingest = self.args.ingest
        
    def logCommandLineSwitches(self):
        """
        Generic method to log the command line switch values

        Arguments:
        <none>
        """
        # Report switch values
        self.log.file(self.progname)
        self.log.file('*** Arguments for vos2caom2 base class ***')
        self.log.file('tools4caom2version = ' + tools4caom2version)
        self.log.file('configpath = ' + self.configpath)
        for attr in dir(self.args):
            if attr != 'id' and attr[0] != '_':
                self.log.file('%-15s= %s' % 
                                 (attr, str(getattr(self.args, attr))))
        self.log.file('workdir = ' + self.workdir)
        self.log.file('local =  ' + str(self.local))
        self.log.file('logdir = ' + self.logdir)
        self.log.console('log = ' + self.logfile)
        
        if self.minor:
            for minor in self.minor:
                self.log.file('minor = ' + minor)
        
        if self.replace:
            for replace in self.replace:
                self.log.file('replace = ' + replace)
        
        self.tap = tapclient(self.log, self.proxy)
        errors = False
        if not os.path.exists(self.proxy):
            errors = True
            self.log.console('ERROR: proxy does not exist: ' + self.proxy)
        
        if not os.path.isdir(self.workdir):
            errors = True
            self.log.console('ERROR: workdir is not a directory: ' + self.workdir)
        
        if self.config and not os.path.isfile(self.config):
            errors = True
            self.log.console('ERROR: config file does not exist: ' + 
                             str(self.config))

        if self.default and not os.path.isfile(self.default):
            errors = True
            self.log.console('ERROR: default file does not exist: ' + 
                             str(self.default))

        if errors:
            self.log.console('Exit due to error conditions',
                             logging.ERROR) 
    
    def commandLineContainers(self):
        """
        Process the list of vos containers (release directories) 

        Arguments:
        <None>

        The list of release directories is passed through the attribute 
        self.voslist. 
        
        If not specified, the name of the log file defaults to <database>.log.
        Beware that log files are always opened in append.  Be sure to delete
        any existing log files if it is important to have a clean record of the
        current run.
        """
        # Find the lists of release directories to ingest.
        self.containerlist = []
        releasedirs = []
        try:
            if self.local:
                if self.minor:
                    for minor in self.minor:
                        releasedirs.append(os.path.abspath(
                                            os.path.expandvars(
                                                os.path.expanduser(
                                                    os.path.join(self.major, 
                                                                 minor)))))
                else:
                    releasedirs.append(os.path.abspath(
                                        os.path.expandvars(
                                            os.path.expanduser(self.major))))
                
                for releasedir in releasedirs:
                    if os.path.isdir(releasedir):
                        filelist = [os.path.join(releasedir, f)
                                   for f in os.listdir(releasedir)]
                        self.containerlist.append(
                            filelist_container(
                                self.log,
                                releasedir,
                                filelist,
                                self.filterfunc,
                                self.make_file_id))
                    else:
                        self.log.console('release is not a directory: ' +
                                         releasedir,
                                         logging.ERROR)
            else:
                if self.minor:
                    for minor in self.minor:
                        releasedirs.append(self.major + '/' + minor)
                else:
                    releasedirs.append(self.major)
                
                for releasedir in releasedirs:
                    if (self.vosclient.access(releasedir) 
                        and self.vosclient.isdir(releasedir)):

                        self.containerlist.append(
                            vos_container(self.log, 
                                          releasedir,
                                          self.archive,
                                          self.ingest,
                                          self.workdir, 
                                          self.dew,
                                          self.vosclient,
                                          self.data_web,
                                          self.make_file_id))
                    else:
                        self.log.console('minor release is not a directory: ' +
                                         minor_release,
                                         logging.ERROR)

        except Exception as e:
            self.log.console(traceback.format_exc(),
                             logging.ERROR)

    #************************************************************************
    # Clear the local plane and artifact dictionaries
    #************************************************************************
    def clear(self):
        """
        Clear the local plane and artifact dictionaries before each file is read.

        Arguments:
        <none>
        """
        self.file_id = ''
        self.uri = ''
        self.observationID = None
        self.productID = None
        self.plane_dict.clear()
        self.fitsuri_dict.clear()
        self.memberset.clear()
        self.member_cache.clear()
        self.inputset.clear()
        self.input_cache.clear()
        self.override_items = 0
        
    #************************************************************************
    # Fill metadict using metadata from each file in the specified container
    #************************************************************************
    def fillMetadict(self, container):
        """
        Generic routine to fill the metadict structure by iterating over
        all containers, extracting the required metadata from each file 
        in turn using fillMetadictFromFile().

        Arguments:
        container: a container of files to read 
        """
        self.metadict.clear()
        self.data_storage.clear()
        self.preview_storage.clear()
        
        try:
            # sort the file_id_list
            file_id_list = container.file_id_list()
            self.log.file('in fillMetadict, file_id_list = ' +
                          repr(file_id_list),
                          logging.DEBUG)

            # Gather metadata from each file in the container
            for file_id in file_id_list:
                self.log.file('In fillMetadict, use ' + file_id,
                              logging.DEBUG)
                with container.use(file_id) as f:
                    if self.mode == 'ingest':
                        self.verifyFileInAD(f)
                    self.fillMetadictFromFile(file_id, f, container)
        finally:
            container.close()
    
    def fillMetadictFromFile(self, file_id, filepath, container):
        """
        Generic routine to read metadata and fill the internal structure
        metadict (a nested set of dictionaries) that will be used to control
        sort and fill the override file templates.

        Arguments:
        file_id : must be added to the header
        filepath : absolute path to the file, must be added to the header
        """
        self.log.file('fillMetadictFromFile: ' + file_id + '  ' + filepath)

        #*****************************************************************
        # Call build_dict to fill plane_dict and fitsuri_dict
        #*****************************************************************
        self.clear()
        # If the file is not a FITS file or is in serious violation of the FITS
        # standard, substitute an empty dictionary for the headers.  This is
        # a silent replacement, not an error, to allow non-FITS files to be
        # ingested allong with regular FITS files.
        if self.dew.namecheck(filepath, report=False):
            try:
                head = pyfits.getheader(filepath, 0)
                head.update('file_id', file_id)
                head.update('filepath', filepath)
                if isinstance(container, vos_container):
                    head.update('DPRCINST', container.vosroot)
                else:
                    head.update('DPRCINST', container.name)
                self.log.file('...got primary header from ' + filepath,
                              logging.DEBUG)
            except:
                head = {}
                head['file_id'] = file_id
                head['filepath'] = filepath
                self.log.file('...could not read primary header from ' + 
                              filepath,
                              logging.DEBUG)
        else:
            self.preview_storage.append(filepath)
        self.file_id = file_id
        self.build_dict(head)
        self.build_metadict(filepath)
        if len(self.dew.errors[filepath]) == 0:
            self.data_storage.append(filepath)
        
    #************************************************************************
    # Format an observation URI for composite members
    #************************************************************************
    def observationURI(self, collection, observationID, member=True):
        """
        Generic method to format an observation URI, i.e. the URI used to
        specify members in a composite observation.

        Arguments:
        collection : the collection containing observationID
        observationID : the observationID of the URI
        member : True => store in memberset

        Returns:
        the value of the observationURI
        """
        uri = ObservationURI('caom:' +
                             collection + '/' +
                             observationID)
        if member:
            self.memberset |= set([uri.uri])
        return uri

    #************************************************************************
    # Format a plane URI for provenance inputs
    #************************************************************************
    def planeURI(self, collection, observationID, productID, input=True):
        """
        Generic method to format a plane URI, i.e. the URI used to access
        a plane in the data repository.

        Arguments:
        collection : the collection containing observationID
        observationID : the observationID containing productID
        productID : the productID of the URI
        input : True => store in inputset

        Returns:
        the value of the planeURI
        """
        uri = PlaneURI('caom:' +
                       collection + '/' +
                       observationID + '/' +
                       productID)
        if input:
            self.inputset |= set([uri.uri])
        return uri

    #************************************************************************
    # Format a URI for data access
    #************************************************************************
    def fitsfileURI(self,
                    archive,
                    file_id,
                    fits2caom2=True):
        """
        Generic method to format an artifact URI, i.e. the URI used to access
        a file in AD.

        Either fitsfileURI or fitsextensionURI must be called with
        fits2caom2=True for every file to be ingested.

        Arguments:
        archive : the archive within ad that holds the file
        file_id : file_id of the file in ad
        fits2caom2 : True => store uri for use with fits2caom2

        Returns:
        the value of the fitsfileURI
        """
        fileuri = 'ad:' + archive + '/' + file_id
        if fits2caom2:
            self.uri = fileuri
            if fileuri not in self.fitsuri_dict:
                self.fitsuri_dict[fileuri] = OrderedDict()
                self.fitsuri_dict[fileuri]['custom'] = OrderedDict()
        return fileuri

    #************************************************************************
    # Format a URI for data access
    #************************************************************************
    def fitsextensionURI(self,
                         archive,
                         file_id,
                         extension_list,
                         fits2caom2=True):
        """
        Generic method to format a part URI, i.e. the URI used to access
        one or more extensions from a FITS file in AD.

        Generating a fitsextensionURI calls fitsfileURI so it is not necessary
        to call both explicitly, but one or the other must be called with
        fits2caom2=True for every file that is ingested.

        Arguments:
        archive : the archive within ad that holds the file
        file_id : file_id of the file in ad
        extension_list : list (or tuple) of integers or tuples containing 
                        integer pairs for the extensions to be ingested; 
                        if omitted ingest all extensions
        fits2caom2 : True => store uri for use with fits2caom2

        Returns:
        the value of the fitsextensionURI
        """
        fileuri = self.fitsfileURI(archive, file_id, fits2caom2=False)
        elist = []
        for e in extension_list:
            if isinstance(e, int):
                elist.append(str(e))
            elif (isinstance(e, tuple) and 
                  len(e) == 2 and
                  isinstance(e[0], int) and
                  isinstance(e[1], int)):
                elist.append(str(e[0]) + '-' + str(e[1]))
            else:
                self.log.console('extension_list must contain only integers '
                                 'or tuples cntaining pairs of integers: ' +
                                 repr(extension_list),
                                 logging.ERROR)
        if elist:
            fexturi = fileuri + '#[' + ','.join(elist) + ']'
            
        if fits2caom2:
            self.uri = fileuri
            if fexturi not in self.fitsuri_dict:
                self.fitsuri_dict[fexturi] = OrderedDict()
                self.fitsuri_dict[fexturi]['custom'] = OrderedDict()
        return fexturi

    #************************************************************************
    # Add a key-value pair to the local plane dictionary
    #************************************************************************
    def add_to_plane_dict(self, key, value):
        """
        Add a key, value pair to the local plane dictionary.  The method will
        throw an exception and exit if the value does not have a string type.

        Arguments:
        key : a key in a string.Template
        value : a string value to be substituted in a string.Template
        """
        if not isinstance(value, str):
            self.log.console("in the (key, value) pair ('%s', '%s'),"
                             " the value should have type 'str' but is %s" %
                             (key, repr(value), type(value)),
                             logging.ERROR)
        self.plane_dict[key] = value
        self.override_items += 1

    #************************************************************************
    # Add a key-value pair to the local fitsuri dictionary
    #************************************************************************
    def add_to_fitsuri_dict(self, uri, key, value):
        """
        Add a key, value pair to the local fitsuri dictionary.  The method
        will throw an exception if the value does not have a string type.

        Arguments:
        uri : the uri of this fits file or extension
        key : a key in a string.Template
        value : a string value to be substituted in a string.Template
        """
        if not isinstance(value, str):
            self.log.console("in the (key, value) pair ('%s', '%s'),"
                             " the value should have type 'str' but is %s" %
                             (key, repr(value), type(value)),
                             logging.ERROR)

        if not uri in self.fitsuri_dict:
            self.log.console('Create the fitsuri before adding '
                             'key,value pairs to the fitsuri_dict: '
                             '["%s"]["%s"] = "%s")' % (uri, key, value),
                             logging.ERROR)

        self.fitsuri_dict[uri][key] = value
        self.override_items += 1

    #************************************************************************
    # Add a key-value pair to the local fitsuri custom dictionary
    #************************************************************************
    def add_to_fitsuri_custom_dict(self, uri, key, value):
        """
        Add a key, value pair to the local fitsuri dictionary.  Unlike the 
        other dictionaries, the fitsuri custom dictionary can hold arbitray
        dictionary values, since the values will be processed using custom
        code and do not necessary get written into the override file.

        Arguments:
        uri : the uri of this fits file or extension
        key : a key
        value : an arbitrary data type
        """
        if not uri in self.fitsuri_dict:
            self.log.console('call fitfileURI before adding '
                             'key,value pairs to the fitsuri_dict: '
                             '["%s"]["%s"] = "%s")' % (uri, key, 
                                                       repr(value)),
                             logging.ERROR)

        self.fitsuri_dict[uri]['custom'][key] = value
        self.override_items += 1

    #************************************************************************
    # Fetch a previously entered value from a specified plane dictionary
    #************************************************************************
    def findURI(self, uri):
        """
        Generic routine to find which collection, observationID and productID
        contains a particular uri that has been previously ingested.

        Arguments:
        uri : an artifact URI to locate

        Returns:
        the tuple (collection, observationID, productID) describing the uri
        or (None, None, None)
        """
        for c in self.metadict:
            for o in self.metadict[c]:
                for p in self.metadict[c][o]:
                    if p not in ['memberset']:
                        if uri in self.metadict[c][o][p]['uri_dict']:
                            return (c, o, p)
        return (None, None, None)

    #************************************************************************
    # Fetch a previously entered value from a specified plane dictionary
    #************************************************************************
    def get_plane_value(self, collection, observationID, productID, key):
        """
        Return the value stored in the plane dictionary for a previously
        entered collection, productID, and key.

        Arguements:
        collection : the collection containing the productID
        productID : the productID containing the key in its plane_dict
        key : the key whose value is needed

        If any of collection, productID or key are not present, a
        KeyError exception will be raised.
        """
        return self.metadict[collection][observationID][productID
                             ]['plane_dict'][key]

    #************************************************************************
    # Fetch a previously entered value from a specified artifact dictionary
    #************************************************************************
    def get_artifact_value(self,
                           collection,
                           observationID,
                           productID,
                           uri,
                           key):
        """
        Return the value stored in the artifact dictionary for a previously
        entered collection, observationID, productID, uri, and key.

        Arguements:
        collection : the collection containing the productID
        productID : the productID containing the key in its plane_dict
        key : the key whose value is needed

        If any of collection, productID or key are not present, a
        KeyError exception will be raised.
        """
        return self.metadict[collection][observationID][productID
                             ]['fitsuri_dict'][uri][key]

    def build_metadict(self, filepath):
        """
        Generic routine to build the internal structure metadict (a nested set
        of ordered dictionaries) that will be used to control, sort and fill
        the override file templates.  The required metadata must already exist
        in the internal structures of vos2caom2.

        Arguments:
        filepath: path to file (may not exist if not local)
        local: True if the file is already on the disk

        The structure of metadict is a nested set of OrderedDict's and sets.
            [collection]
                [observationID]
                    ['memberset']
                    [productID]
                        ['uri_dict']
                        ['inputset']
                        ['plane_dict']
                        [fitsuri]
                            ['custom']
        where:
            - The metadict is an OrderedDict of collections.
            - Each collection is an OrderedDict of observations.
            - Each observation is an OrderedDict of planes.
            - Each observation also contains an element called 'memberset'
              holding the set of members for the observation, which will be
              empty for a simple observation.
            - Each plane is an OrderedDict containing a set of fitsuri dicts.
            - Each plane contains an element 'uri_dict' that holds an 
              OrderedDict of input URIs to pass to fits2caom2.  The uri is the 
              key into the dictionary, where the value is the path to the file
              if it is local or None if it should be fetched from AD.
            - Each plane contains an element 'inputset' that holds a set of
              provenance input URIs for this plane, which can be empty.
            - Each plane also contains an element 'plane_dict' that is an
              OrderedDict holding items to add to the plane part of the
              override file.  The 'plane_dict' can be empty.
            - Each fitsuri dict is an OrderedDict containing items to include
              in the override file for that fitsuri.
            - The "custom" item inside the fitsuri is an OrderedDict of
              items that will be used to create archive-specific
              structures in the "science" chunks of an artifact.
              Archive-specific code should override the
              build_fitsuri_custom() method.
        """
        self.log.file('build_metadict',
                      logging.DEBUG)
        
        # In check mode, errors should not raise exceptions
        raise_exception = True
        if self.mode in ('new', 'replace', 'check'):
            raise_exception = False
        
        #If the plane_dict is completely empty, skip further processing
        if self.override_items:
            #*****************************************************************
            # fetch the required keys from self.plane_dict
            #*****************************************************************
            if not self.collection:
                if raise_exception:
                    self.log.console(filepath + ' does not define the required'
                                     ' key "collection"',
                                     logging.ERROR)
                else:
                    return

            if not self.observationID:
                if raise_exception:
                    self.log.console(filepath + ' does not define the required'
                                     ' key "observationID"',
                                     logging.ERROR)
                else:
                    return

            if not self.productID:
                if raise_exception:
                    self.log.console(filepath + ' does not define the required' +
                                     ' key "productID"',
                                     logging.ERROR)
                else:
                    return

            if not self.uri:
                if raise_exception:
                    self.log.console(filepath + ' does not call fitsfileURI()'
                                     ' or fitsextensionURI()',
                                     logging.ERROR)
                else:
                    return

            self.log.file(('PROGRESS: collection="%s" observationID="%s" '
                           'productID="%s"') % (self.collection,
                                                self.observationID,
                                                self.productID))

            #*****************************************************************
            # Build the dictionary structure
            #*****************************************************************
            if self.collection not in self.metadict:
                self.metadict[self.collection] = OrderedDict()
            thisCollection = self.metadict[self.collection]

            if self.observationID not in thisCollection:
                thisCollection[self.observationID] = OrderedDict()
            thisObservation = thisCollection[self.observationID]

            #*****************************************************************
            # If memberset is not empty, the observation is a composite.
            # The memberset is the union of the membersets from all the
            # files in the observation.
            #*****************************************************************
            if 'memberset' not in thisObservation:
                thisObservation['memberset'] = set([])
            if self.memberset:
                thisObservation['memberset'] |= self.memberset

            #*****************************************************************
            # Create the plane-level structures
            #*****************************************************************
            if self.productID not in thisObservation:
                thisObservation[self.productID] = OrderedDict()
            thisPlane = thisObservation[self.productID]

            #*****************************************************************
            # Items in the plane_dict accumulate, but the last item is used
            #*****************************************************************
            if 'plane_dict' not in thisPlane:
                thisPlane['plane_dict'] = OrderedDict()
            if self.plane_dict:
                for key in self.plane_dict:
                    thisPlane['plane_dict'][key] = self.plane_dict[key]

            #*****************************************************************
            # If inputset is not empty, the provenance should be filled.
            # The inputset is the union of the inputsets from all the files
            # in the plane
            #*****************************************************************
            if 'inputset' not in thisPlane:
                thisPlane['inputset'] = set([])
            if self.inputset:
                thisPlane['inputset'] |= self.inputset

            #*****************************************************************
            # Record the uri and (optionally) the filepath 
            #*****************************************************************
            if 'uri_dict' not in thisPlane:
                thisPlane['uri_dict'] = OrderedDict()
            if self.uri not in thisPlane['uri_dict']:
                if self.local:
                    thisPlane['uri_dict'][self.uri] = filepath
                else:
                    thisPlane['uri_dict'][self.uri] = None

            #*****************************************************************
            # Foreach fitsuri in fitsuri_dict, record the metadata
            #*****************************************************************
            for fitsuri in self.fitsuri_dict:
                #*********************************************************
                # Create the fitsuri-level structures
                #*********************************************************
                if fitsuri not in thisPlane:
                    thisPlane[fitsuri] = OrderedDict()
                    thisPlane[fitsuri]['custom'] = OrderedDict()
                thisFitsuri = thisPlane[fitsuri]

                #*********************************************************
                # Copy the fitsuri dictionary
                #*********************************************************
                for key in self.fitsuri_dict[fitsuri]:
                    if key == 'custom':
                        thisCustom = thisFitsuri[key]
                        for customkey in self.fitsuri_dict[fitsuri][key]:
                            thisCustom[customkey] = \
                                self.fitsuri_dict[fitsuri][key][customkey]
                    else:
                        thisFitsuri[key] = self.fitsuri_dict[fitsuri][key]

    def verifyFileInAD(self, file_id):
        """
        Use the data_web client to verify that file_id is in self.archive
        """
        found = False
        if self.data_web(self.archive, file_id):
            found = True
        return found
    
    def storeFiles(self):
        """
        If files approved for storage are in vos, create a link in the
        VOS pickup directory.  
        """
        if (self.userconfig.has_section('vos')
            and self.userconfig.has_option('vos', 'pickup')):
            pickup = self.userconfig.get('vos', 'pickup')
            
            for filelist in (self.data_storage, self.preview_storage):
                for filepath in filelist:
                    basefile = os.path.basename(filepath)
                    self.vosclient.link(filepath, pickup + '/' + basefile)
    
    def checkMembers(self):
        """
        Checking membership will be archive-specific
        """
        pass

    def checkProvenanceInputs(self):
        """
        Checking provenance inputs will be archive-specific
        """
        pass

    #************************************************************************
    # Write the override file for a plane
    #************************************************************************
    def writeOverrideFile(self, collection, observationID, productID):
        """
        Generic method to write override files for a plane specified
        by the collection, observationID and productID.

        Arguments:
        collection : the collection containing observationID
        observationID : the observationID containing productID
        productID : productID for this plane

        Returns:
        filepath for override file
        """
        filepath = os.path.join(self.workdir,
                                '_'.join([collection,
                                          observationID,
                                          productID]) + '.override')
        with open(filepath, 'w') as OVERRIDE:
            thisObservation = self.metadict[collection][observationID]
            thisPlane = thisObservation[productID]

            for key in thisPlane['plane_dict']:
                print >>OVERRIDE, \
                    '%-30s = %s' % (key, thisPlane['plane_dict'][key])

            # Write artifact-specific overrides
            for fitsuri in thisPlane:
                if fitsuri not in ('uri_dict',
                                  'inputset',
                                  'plane_dict'):
                    thisFitsuri = thisPlane[fitsuri]
                    print >>OVERRIDE
                    print >>OVERRIDE, '?' + fitsuri
                    for key in thisFitsuri:
                        if key != 'custom':
                            print >>OVERRIDE, \
                                '%-30s = %s' % (key, thisFitsuri[key])
        return filepath

    #************************************************************************
    # Run fits2caom2.
    # If an error occurs, rerun in debug mode.
    #************************************************************************
    def runFits2caom2(self, collection,
                            observationID,
                            productID,
                            xmlfile,
                            overrideFile,
                            uristring,
                            localstring,
                            arg='',
                            debug=False):
        """
        Generic method to format and run the fits2caom2 command.

        Arguments:
        collection    : CAOM collection for this observation
        observationID : CAOM observationID for this observation
        productID     : CAOM productID for this plane
        overrideFile  : path to override file
        uristring     : (string) comma-separated list of file URIs
        arg           : (string) additional fits2caom2 switches
        debug         : (boolean) include --debug switch by default

        If fits2caom2 fails, the command will be run again with the additional
        switch --debug, to capture in the log file details necessary to
        debug the problem.
        """

        # build the fits2caom2 command

        if self.big:
            cmd = ('java -Xmx512m -jar ${CADC_ROOT}/lib/fits2caom2.jar ' + 
                   self.local_args)
        else:
            cmd = ('java -Xmx128m -jar ${CADC_ROOT}/lib/fits2caom2.jar ' + 
                   self.local_args)

        cmd += ' --collection="' + collection + '"'
        cmd += ' --observationID="' + observationID + '"'
        cmd += ' --productID="' + productID + '"'

        if os.path.exists(xmlfile):
            cmd += ' --in="' + xmlfile + '"'
        cmd += ' --out="' + xmlfile + '"'

        cmd += ' --config="' + self.config + '"'
        cmd += ' --default="' + self.default + '"'
        cmd += ' --override="' + overrideFile + '"'
        cmd += ' --uri="' + uristring + '"'
        if self.local:
            cmd += ' --local="' + localstring + '"'

        if self.logfile:
            cmd += ' --log="' + self.logfile + '"'

        if debug:
            cmd += ' --debug'

        if arg:
            cmd += ' ' + arg

        # run the command
        self.log.file("fits2caom2Interface: cmd = '" + cmd + "'")
        if not self.test:
            cwd = os.getcwd()
            try:
                # create a temporary working directory
                tempdir = tempfile.mkdtemp(dir=self.workdir)
                os.chdir(tempdir)
                status, output = commands.getstatusoutput(cmd)

                # if the first attempt to run fits2caom2 fails, try again with
                # --debug to capture the full error message
                if status:
                    self.errors = True
                    self.log.console("fits2caom2 return status %d" % (status))
                    if not debug:
                        self.log.console("fits2caom2 - rerun in debug mode")
                        cmd += ' --debug'
                        status, output = commands.getstatusoutput(cmd)
                    self.log.console("output = '%s'" % (output), 
                                     logging.ERROR)
                elif debug:
                    self.log.file("output = '%s'" % (output))
            finally:
                # clean up FITS files that were not present originally 
                os.chdir(cwd)
                shutil.rmtree(tempdir)

    #************************************************************************
    # Add members to the observation xml
    #************************************************************************
    def replace_members(self, thisObservation, thisPlane):
        """
        For the current plane, insert the full set of members in the plane_dict
        
        Arguments:
        collection: the collection for this plane
        observationID: the observationID for this plane
        productID: the the productID for this plane
        """
        memberset = thisObservation['memberset']
        if 'algorithm.name' in thisPlane['plane_dict']:
            self.log.console('replace_members: algorithm.name = ' + 
                             thisPlane['plane_dict']['algorithm.name'],
                             logging.DEBUG)
                             
            self.log.console('memberset = ' + repr(list(memberset)),
                             logging.DEBUG)
            if (memberset and 
                thisPlane['plane_dict']['algorithm.name'] != 'exposure'):
                
                thisPlane['plane_dict']['members'] = ' '.join(
                                            sorted(list(memberset)))

    #************************************************************************
    # Add inputs to a plane in an observation xml
    #************************************************************************
    def replace_inputs(self, thisObservation, thisPlane):
        """
        For the current plane, insert the full set of inputs in the plane_dict
        
        Arguments:
        collection: the collection for this plane
        observationID: the observationID for this plane
        productID: the the productID for this plane
        """
        if 'provenance.name' in thisPlane['plane_dict']:
            inputset = thisPlane['inputset']
            self.log.console('replace_inputs: provenance.name = ' + 
                             thisPlane['plane_dict']['provenance.name'],
                             logging.DEBUG)
                             
            self.log.console('inputset = ' + repr(list(inputset)),
                             logging.DEBUG)
            if inputset:
                thisPlane['plane_dict']['provenance.inputs'] = ' '.join(
                                            sorted(list(inputset)))

    #************************************************************************
    # Ingest planes from metadict, tracking members and inputs
    #************************************************************************
    def ingestPlanesFromMetadict(self):
        """
        Generic routine to ingest the planes in metadict, keeping track of
        members and inputs.

        Arguments:
        <none>
        """
        # Try a backoff that is much longer than usual
        repository = Repository(self.workdir, 
                                self.log, 
                                debug=self.debug,
                                backoff=[10.0, 20.0, 40.0, 80.0])

        for collection in self.metadict:
            thisCollection = self.metadict[collection]
            for observationID in thisCollection:
                obsuri = self.observationURI(collection,
                                             observationID,
                                             member=False)
                with repository.process(obsuri) as xmlfile:

                    thisObservation = thisCollection[observationID]
                    for productID in thisObservation:
                        if productID != 'memberset':
                            thisPlane = thisObservation[productID]

                            self.log.console('PROGRESS ingesting '
                                             'collection="%s"  '
                                             'observationID="%s" '
                                             'productID="%s"' %
                                                    (collection,
                                                     observationID,
                                                     productID))
                            
                            self.replace_members(thisObservation,
                                                 thisPlane)

                            self.replace_inputs(thisObservation,
                                                thisPlane)

                            override = self.writeOverrideFile(collection,
                                                              observationID,
                                                              productID)

                            #********************************************
                            # Run fits2caom2 and record the planeID
                            #********************************************
                            urilist = sorted(thisPlane['uri_dict'].keys())
                            if urilist:
                                uristring = ','.join(urilist)
                                localstring = ''
                                if thisPlane['uri_dict'][urilist[0]]:
                                    filepathlist = [thisPlane['uri_dict'][u] 
                                                    for u in urilist]
                                    localstring = ','.join(filepathlist)
                            else:
                                self.log.console('for ' + collection +
                                                 '/' + observationID +
                                                 '/' + planeID + 
                                                 ', uri_dict is empty so '
                                                 'there is nothing to ingest',
                                                 logging.ERROR)

                            arg = thisPlane.get('fits2caom2_arg', '')

                            try:
                                self.runFits2caom2(collection,
                                                   observationID,
                                                   productID,
                                                   xmlfile,
                                                   override,
                                                   uristring,
                                                   localstring,
                                                   arg=arg,
                                                   debug=self.switches.debug)
                                self.log.file('INGESTED: observationID=%s '
                                              'productID="%s"' %
                                                    (observationID, productID))
                            finally:
                                if not self.debug:
                                    os.remove(override)

                            for fitsuri in thisPlane:
                                if fitsuri not in ('plane_dict',
                                                   'uri_dict',
                                                   'inputset'):

                                    self.build_fitsuri_custom(xmlfile,
                                                              collection,
                                                              observationID,
                                                              productID,
                                                              fitsuri)

                            self.build_plane_custom(xmlfile,
                                                    collection,
                                                    observationID,
                                                    productID)

                    self.build_observation_custom(xmlfile,
                                                  collection,
                                                  observationID)

                self.log.console('SUCCESS observationID="%s"' %
                                    (observationID))

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
        self.ap = argparse.ArgumentParser(self.progname)
        self.defineCommandLineSwitches()
        
        self.args = self.ap.parse_args()
        self.processCommandLineSwitches()

        with logger(self.logfile,
                    loglevel=self.loglevel).record() as self.log:
            self.logCommandLineSwitches()
            # Read list of files from VOspace and do things
            try:
                self.data_web = data_web_client(self.workdir, self.log)
                # It is harmless to create a database connection object if it
                # is not going to be used, since the actual connections use
                # lazy initialization and are not opened until a call to read 
                # or write is made.
                with connection(self.userconfig, 
                                self.log) as self.conn, \
                     dew(self.log, 
                         self.workdir, 
                         self.archive,
                         self.fileid_regex_dict,
                         make_file_id).gather() as self.dew:
                    
                    self.commandLineContainers()
                    for c in self.containerlist:
                        self.log.console('PROGRESS: container = ' + c.name)
                        self.fillMetadict(c)
                        self.checkMembers()
                        self.checkProvenanceInputs()
                        if self.dew.error_count == 0:
                            if self.store:
                                self.storeFiles()
                            elif self.ingest:
                                self.ingestPlanesFromMetadict()

                # if no errors, declare we are DONR
                self.log.console('DONE')
            except Exception as e:
                self.errors = True
                if not isinstance(e, logger.LoggerError):
                    # Log this previously uncaught error, but let it pass
                    try:
                        self.log.console(traceback.format_exc(),
                                         logging.ERROR)
                    except Exception as p:
                        pass

if __name__ == '__main__':
    vc = vos2caom2()
    vc.run()