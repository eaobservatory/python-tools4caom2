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

from tools4caom2.database import connection
from tools4caom2.data_web_client import data_web_client
from tools4caom2.delayed_error_warning import delayed_error_warning as dew
from tools4caom2.ingest2caom2 import ingest2caom2
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

def fits_png_filter(filepath):
    """
    Return True if this file should be processeded, False otherwise.
    This is a default function that accepts FITS and PNG files, where the FITS
    files hold processed data and the PNG files hold preview thumbnails. 

    Arguments:
    filepath : the file name to check for validity
    """
    return (os.path.splitext(filename)[1].lower() in
            ['.fits', '.fit', '.png'])


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
                ingest2caom2.__init__(self)
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
        self.archive = None
        self.stream = None
        self.schema = None
        self.collection_choices = ['SANDBOX']
        
        # routine to convert filepaths into file_ids
        self.make_file_id = make_file_id

        # temporary disk space for working files
        self.outdir = None

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
        self.filterfunc = fits_png_filter

        # Ingestion parameters and structures
        self.mode = None
        
        # Archive-specific fits2caom2 config and default file paths
        self.config = None
        self.default = None
        
        # A list of vos containers to ingest
        self.voslist = []
        # Current vos container
        self.vos = None
        
        # Working structures
        # The metadata dictionary - fundamental structure for the entire class
        # For the detailed structure of metadict, see the help text for
        # fillMetadictFromFile()
        self.metadict = OrderedDict()
        
        # Delayed errors and warnings discovered in files
        self.dew = None
        self.namecheck_regex_list = [re.compile(r'.*')]
        
        # TAP client
        self.tap = tapclient()

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

        # config and mode arguments
        --userconfig : path to user configuration file
        --proxy      : path to CADC proxy certificate
        --mode       : one of ("new", "check", "store", "persist")

        # fits2caom2 options
        --collection : (required) collection to use for ingestion
        --config     : (optional) path to fits2caom2 config file
        --default    : (optional) path to fits2caom2 default file

        # File and directory options
        --outdir     : output (working) directory (default = current directory)
        --local      : the "release directories" are actaually on the disk

        # debugging options
        --log        : (optional) name of the log file
        --logdir     : directory to hold log files
        --debug      : (optional) log all messages and retain temporary files
                       on error
        --test       : (optional) simulate operation of fits2caom2

        Any additional arguments are interpreted as a list of VOspace directories
        to ingest.  The format must be vos:<path_to_release_directory> unless the
        --local switch is used.

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

        self.ap.add_argument('--mode',
            choices=['new', 'check', 'store', 'ingest'],
            default='new',
            help='ingestion mode')

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
        self.ap.add_argument('--outdir',
            help='output directory, (default = current directory')
        self.ap.add_argument('--local',
            action='store_true',
            help='release directories are on the local disk')

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

        self.ap.add_argument('voslist',
            nargs='*',
            help='VOspace release directories to ingest')

    
    def processCommandLineSwitches(self):
        """
        Generic routine to process the command line arguments
        and create outdir if necessary.  This will check the values of the
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
        
        self.mode = self.args.mode

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

        if self.args.outdir:
            self.outdir = os.path.abspath(
                             os.path.expandvars(
                                 os.path.expanduser(self.args.outdir)))
        else:
            self.outdir = os.getcwd()
        
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

        logbase = self.progname
        if len(self.args.voslist) == 1:
            logbase = re.sub(r'[^a-zA-Z0-9]', r'_', 
                             os.path.splitext(
                                 os.path.basename(
                                     self.args.voslist[0]))[0])
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
        
        # create outdir if it does not already exist
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

        if len(self.args.voslist) > 0:
            self.voslist = self.args.voslist
    
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
        self.log.file('outdir = ' + self.outdir)
        self.log.file('local =  ' + str(self.local))
        self.log.file('logdir = ' + self.logdir)
        self.log.console('log = ' + self.logfile)
        
        errors = False
        if not os.path.exists(self.proxy):
            errors = True
            self.log.console('ERROR: proxy does not exist: ' + self.proxy)
        
        if not os.path.isdir(self.outdir):
            errors = True
            self.log.console('ERROR: outdir is not a directory: ' + self.outdir)
        
        if len(self.args.voslist) == 0:
            errors = True
            self.log.console('ERROR: no vos release directories to ingest')

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
        try:
            if local:
                for localdir in self.voslist:
                    if os.path.isdir(localdir):
                        absf = os.path.abspath(
                                    os.path.expandvars(
                                        os.path.expanduser(localdir)))
                        dirlist = [os.path.join(absf, ff)
                                   for ff in os.listdir(absf)]
                        self.containerlist.append(
                            filelist_container(
                                self.log,
                                os.path.basename(absf),
                                dirlist,
                                lambda f : (self.dew.namecheck(f)
                                            and self.dew.zerolength(f)),
                                self.make_file_id))
            else:
                for vos in self.voslist:
                    if re.match(r'^vos:([\S]+$)', vos):
                        self.log.console('vos container: ' + vos,
                                         logging.DEBUG)
                        
                        self.containerlist.append(
                            vos_container(self.log, 
                                          self.data_web,
                                          vos,
                                          self.archive,
                                          self.mode,
                                          self.dew,
                                          self.outdir, 
                                          self.make_file_id))

        except Exception as e:
            self.log.console(traceback.format_exc(),
                             logging.ERROR)

    #************************************************************************
    # Fill metadict using metadata from each file in the specified container
    #************************************************************************
    def fillMetadict(self, container):
        """
        Generic routine to fill the metadict structure by iterating over
        all containers (only vos_containers in this application),
        extracting the required metadata from each file in turn using 
        fillMetadictFromFile().

        Arguments:
        <none>
        """
        try:
            # sort the file_id_list
            file_id_list = container.file_id_list()
            self.log.file('in fillMetadict, file_id_list = ' +
                          repr(file_id_list),
                          logging.DEBUG)

            # verify that each file is in ad and ingest its metadata
            for file_id in file_id_list:
                self.log.file('In fillMetadict, use ' + file_id,
                              logging.DEBUG)
                with container.use(file_id) as f:
                    if self.mode == 'ingest':
                        self.verifyFileInAD(f)
                    self.fillMetadictFromFile(file_id, f)
        finally:
            container.close()
    
    def fillMetadictFromFile(self, file_id, filepath):
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
        try:
            head = pyfits.getheader(filepath, 0)
            head.update('file_id', file_id)
            head.update('filepath', filepath)
            self.log.file('...got primary header from ' + filepath,
                          logging.DEBUG)
        except:
            head = {}
            head['file_id'] = file_id
            head['filepath'] = filepath
            self.log.file('...could not read primary header from ' + filepath,
                          logging.DEBUG)
        self.file_id = file_id
        self.build_dict(head)
        self.build_metadict(filepath, local)
        
    def build_metadict(self, filepath, local):
        """
        Generic routine to build the internal structure metadict (a nested set
        of ordered dictionaries) that will be used to control, sort and fill
        the override file templates.  The required metadata must already exist
        in the internal structures of ingest2caom2.

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
        #If the plane_dict is completely empty, skip further processing
        if self.override_items:
            #*****************************************************************
            # fetch the required keys from self.plane_dict
            #*****************************************************************
            if not self.collection:
                self.log.console(filepath + ' does not define the required'
                                 ' key "collection"',
                                 logging.ERROR)

            if not self.observationID:
                self.log.console(filepath + ' does not define the required'
                                 ' key "observationID"',
                                 logging.ERROR)

            if not self.productID:
                self.log.console(filepath + ' does not define the required' +
                                 ' key "productID"',
                                 logging.ERROR)

            if not self.uri:
                self.log.console(filepath + ' does not call fitsfileURI()'
                                 ' or fitsextensionURI()',
                                 logging.ERROR)

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
                if local:
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


    def verifyFileInAD(self):
        pass
    
    def storeFiles(self, container):
        pass
    
    def ingestPlanesFrommetadict(self):
        pass
    
    
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
                self.data_web = data_web_client(self.outdir, self.log)
                # It is harmless to create a database connection object if it
                # is not going to be used, since the actual connections use
                # lazy initialization and are not opened until a call to read 
                # or write is made.
                with connection(self.userconfig, 
                                self.log) as self.conn, \
                     dew(self.log, 
                         self.outdir, 
                         self.archive, 
                         self.namecheck_regex_list,
                         make_file_id).gather() as self.dew:
                    
                    self.commandLineContainers()
                    for c in self.containerlist:
                        self.log.console('PROGRESS: container = ' + c.name)
                        self.fillMetadict(c)
                        if self.dew.error_count == 0:
                            if self.mode == 'store':
                                self.storeFiles()
                            elif self.mode == 'ingest':
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