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
#*   Script Name:    ingest2caom2.py
#*
#*   Purpose:
#*    Organize the ingestion of data files into CAOM using fits2caom2
#*
#*   Classes:
#+    ingest2caom2                    : Ingest files into CAOM using fits2caom2
#+                                    : To be used as a base class for
#+                                      CAOM ingestions into specific archives
#*
#*   Functions:
#+    Redefine these class methods in a derived class for a specific archive
#+
#+    ingest2caom2.build_dict         : Build the substitution dictionary for
#+                                      each file
#+    Optionally, define a filter function (outside the class):
#+    filter(f)                       : return True if f is a file to ingest,
#+                                      False otherwise
#+    Optionally define a file_id comparison function (outside the class)
#+    in one of two ways:
#+    gt(f1, f2)                      : return True if f1 should be after f2,
#+                                      False otherwise
#+    compare(f1,f2)                  : return 1 if f1 should come after f2,
#+                                      -1 if f1 should come before f2, and
#+                                      0 if the order is not significant
#+    These can be used to initialize the fields filterfunc and either
#+    gtfunc or cmpfunc in the __init__ function for the derived class.
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
__author__ = "Russell O. Redman"

import argparse
import commands
from ConfigParser import SafeConfigParser
from contextlib import contextmanager
from collections import OrderedDict
import datetime
import errno
import exceptions
import logging
import optparse
import os
import os.path
import pyfits
import re
import shutil
import stat
import string
import subprocess
from subprocess import CalledProcessError
import sys
import tarfile
import tempfile
import time
import traceback

from caom2.xml.caom2_observation_reader import ObservationReader
from caom2.xml.caom2_observation_writer import ObservationWriter

from caom2.caom2_composite_observation import CompositeObservation
from caom2.caom2_observation_uri import ObservationURI
from caom2.caom2_plane_uri import PlaneURI

from tools4caom2 import __version__
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.gridengine import gridengine
from tools4caom2.logger import logger
from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.adfile_container import adfile_container
from tools4caom2.dataproc_container import dataproc_container
from tools4caom2.filelist_container import filelist_container
from tools4caom2.tarfile_container import tarfile_container

from jcmt2caom2.jsa.utdate_string import utdate_string

__doc__ = """
The ingest2caom2 base class supplies a generic wrapper for fits2caom2.
For each new archive, derive a new class that customizes the methods:
 class archive2caom2(ingest2caom2):
 - __init__         : supply archive-specific default values
 - build_dict       : given the headers from a FITS file, define plane and
                      uri dependent data structures
Optionally, it may be useful to customize the methods:
 - build_observation_custom : modify the xml file after all fits2caom2 
                              operations on an observation are complete
 - build_plane_custom : modify the xml file after each fits2caom2 
                        operations is complete
The latter two calls allow, for example, the time bounds derived from raw
data to be added to the science chunks within a composite observation.

It might also be useful to define filter and comparison functions (outside
the class):
 - archivefilter(f)                : return True if f is a file to ingest,
                                            False otherwise
 - archivegt(f1, f2)               : return True if f1 should come after f2,
                                            False otherwise
 - archivecompare(f1,f2)           : return 1 if f1 should come after f2,
                                           -1 if f1 should come before f2, and
                                            0 if the order is not significant
These can be used to initialize the fields filterfunc and either gtfunc
or cmpfunc in the __init__ method of the derived class.  The ingest2caom2 
module supplies examples of these functions that are adequate for mamny 
purposes:
 - fitsfilter(f)                   : return True if f is a FITS file,
                                            False otherwise
 - nofilter(f)                     : return True always, i.e. no filtering
 - nosort(f1, f2)                  : returns False always, i.e. no sorting
The defaults for the filterfunc and cmpfunc use fitsfilter and nosort.

It is sometimes also useful to supply a custom function
 - make_file_id(f)                 : given a file name, return an AD file_id

The commandLineSwitches method inherited from ingest2caom2 defines a common
command line interface that should be adequate for all but the most complex
archives.  By overriding this method, it is possible to add more switches
that can be queried in the build routines.

It will often be useful to define a command line program (without the .py
extension) that contains nothing but code of the form:
    from archive2caom2.archive2caom2 import archive2caom2

    if __name__ == '__main__':
        mya2c = archive2caom2()
        mya2c.run()

                                                           
Outside of these methods and functions, there should be no need for further 
customization.  The rest of the code needed to submit the job to gridengine, s
tore the files in ad with adPut, fetch the files with adGet, create the 
override files and run fits2caom2 is handled by the generic methods of the 
ingest2caom2 class. 

Version: """ + __version__.version

#************************************************************************
#* Utility routines
#************************************************************************
def make_file_id(basename):
    """
    An archive-specific routine to convert a file basename (without the
    directory path) to the corressponding file_id used in AD.  The default
    routine provided here strips off the extension, but otherwise leaves
    the basename unchanged.  In some archives, the name might be put into
    lower case, or parts of the name may be extracted to make the file_id.
    This routine or its equivalent must be supplied as an argument for most
    file containers.

    Arguments:
    basename : a file name without the directory path
    This is a static method taking exactly one argument.
    """
    return os.path.splitext(basename)[0]


def fitsfilter(filename):
    """
    Return True if this file should be ingested, False otherwise.
    By default, only ingest FITS files.  The filter will only be applied
    to files in a directory, tar file or file list, not to file_id's in
    an AD file.

    Arguments:
    filename : the file name to check for validity
    This is a static method taking exactly one argument.
    """
    return (os.path.splitext(string.lower(filename))[1] in
            ['.fits', '.fit'])


def nofilter(filename):
    """
    Return True always, so no files are filered out.

    Arguments:
    filename : the file name to check for validity
    This is a static method taking exactly one argument.
    """
    return True


def nosort(file_id1, file_id2):
    """
    Compare two file_ids, returning True if file_id1 should appear after
    file_id2 (analogous to a > operator).  See cgps2caom2 and
    blast2caom2 for examples. Note that file_id's do not generally include
    extensions.

    Always returning False will leave the file_id's unsorted.

    Arguments:
    file_id1 : the first file_id
    file_id2 : the second file_id
    This is a static method taking exactly two arguments.
    """
    return False


#************************************************************************
#* class ingest2caom2 mplements the ingestion of a set of files using
#* fits2caom2
#************************************************************************
class ingest2caom2(object):
    """
    Base class for the ingestion of sets of files into CAOM using fits2caom2
    """
    # The presence of this rejection key in plane_dict will prevent a
    # plane from being created/updated.
    REJECT = 'reject_this_plane'

    #************************************************************************
    # Archive-specific methods to write override files; these methods need
    # to be overridden in derived classes for particular archives.
    #
    # Note that values in these dictionaries are strings or lists of strings.
    # Convert integer and floating point values to strings before use.
    # PyFITS converts FITS header values to numerical values automatically.
    #************************************************************************
    def build_dict(self, header):
        """
        Archive-specific method to fill and return a dictionary (commonly
        called mydict) from the file header.

        Arguments:
        header   : Normally a pyfits header structure, but can be a dictionary
                   containing header values.

        The build_dict method builds the plane and fitsuri dictionaries that
        will be used to write entries into the override file.  The method
        requires that, for each input file, values be set for
            self.collection
            self.observationID
            self.productID

        For every URI to be ingested, it is necessary to call one of
            artifactURI = self.fitsfileURI(archive,
                                           file_id,
                                           fits2caom2=True)
        or
            partURI = self.fitsextensionURI(archive,
                                            file_id,
                                            extension_list=[],
                                            fits2caom2=True)
        The argument "extension" is a list of integers indicatng
        which extensions are to be ingested using this partURI.
        
        To add a key=value pair to the plane section of the override file, call:
            self.add_to_plane_dict(key, value)
        To add a key=value pair to the artifact- or part-specific section of 
        the override file, call:
            self.add_to_fitsuri_dict(URI, key, value)
        where URI is an artifactURI or a partURI.  Part-specific sections
        should be defined before the artifact-specific section that contains
        the part, since ingest2caom2 remembers the order of definition and 
        will write the sections in the same order in the override file.

        If fits2caom2=True, the fitsfileURI and fitsextensionURI methods 
        record an artifactURI in the set of inputURIs to be passed to 
        fits2caom2 for ingestion. This can be prevented by setting the 
        argument fits2caom2=False.
        
        For example:
           for extension in (0, 1):
               uri = self.fitsextensionURI(self.archive,
                                           header['file_id'],
                                           (extension,))
               if extension:
                   self.add_to_fitsuri_dict(uri, 'foo', 'foovalue')
               else:
                   self.add_to_fitsuri_dict(uri, 'bar', 'barvalue')

        The value of every entry in the plane_dict and fitsuri_dict MUST be a
        string, so that formatting decisions are made in the archive-specific
        code and not postponed to generic code that may not be able to handle
        the additional complexity.
        
        To add an observation to the list of members (i.e. to make the 
        observation a composite), call
            observationURI = self.observationURI(collection,
                                                 observationID,
                                                 member=True)
        To calculate the observationURI without adding it to the list of
        members, set member=False.
        
        Similarly, to add a plane to the set of provenance inputs, call
            planeURI = self.planeURI(collection,
                                     observationID,
                                     productID,
                                     input=True)
        To calculate the planeURI without adding it to the list of
        provenance inputs, set input=False.
        """
        # Customize this code to fill in the structural keyword/value pairs.

        # The code assumes that all the required header information is
        # available from the primary HDU, whose header is passed as an
        # argument, but if this is not true the filepath is available
        # as header['filepath'] and can be used with PyFITS to extract
        # other header information.

        # Additional header information is available from any file that
        # has previously been ingested using the methods
        #    self.get_plane_value(collection, productID, key)
        #    self.get_artifact_value(collection, productID, uri, key)

        # A collection identifies major subsets of an archive.
        self.collection = 'dummy'

        # The observationID uniquely identifies an observation within a
        # collectionID.  The combination collection/observationID uniquely
        # identifies the observation within the whole set of CAOM archives.
        self.observationID = 'dummy'

        # The productID uniquely identifies a plane within an observation.
        self.productID = 'dummy'

        # If the observation is 'composite', append member URIs using calls to
        # the method observationURI(). The membership accumulates as a set
        # (no duplicates) and need not be identical for all artifacts,
        # although it normally will be.
        # self.observationURI(collection, observationID)

        # If this plane has inputs, append input URIs using calls to the method
        # planeURI().  The inputs accumulate as a set (no duplicates) and
        # need not be identical for all artifacts, although they will be
        # normally.
        # self.planeURI(collection, observationID, productID)

        # Every file to be ingested must be passed to fits2caom2 using an
        # artifact URI generated by the
        # self.fitsfileURI(archive,
        #                  file_id,
        #                  extension)
        # where extension is an optional list or tuple of integers indicating
        # which extensions are to be ingested.  For a simple example in which
        # every extension is to be ingested:
        # archive = 'dummy'
        # file_id = header['file_id']
        # self.fitsfileURI(archive,
        #                  file_id)
        # Note that the file_id will be in header even if it is not a
        # FITS header in the file/extension.

    #************************************************************************
    # Optionally read user configuration 
    #************************************************************************
    def read_user_config(self, userconfigpath):
        """
        If a user configuration file has been specified, read it and
        copy the configuration into the appropriate files in self.
        
        Arguments:
        userconfigpath: path to user configuration file
        """
        if os.path.isfile(userconfigpath):
            config_parser = SafeConfigParser()
            with open(userconfigpath) as UC:
                config_parser.readfp(UC)
        
            if config_parser.has_section('database'):
                for option in config_parser.options('database'):
                    self.userconfig[option] = config_parser.get('database', 
                                                                option)

    #************************************************************************
    # Apply archive-specific changes to a plane in an observation xml
    #************************************************************************
    def build_observation_custom(self, 
                                 xmlfile, 
                                 collection,
                                 observationID):
        """
        For the observation described in xmlfile and for the requested plane
        apply archive-specific modifications.  This can use metadata stored 
        in the plane "custom" dictionary.
        
        Arguments:
        xmlfile: path to the xmlfile that needs to be modified
        collection: collection to edit
        observationID: the observationID to edit
        """
        pass
#        observation = self.reader.read(xmlfile)
#        change = False
#        <implement the changes>
#        if change:
#            with open(xmlfile, 'w') as XMLFILE:
#                self.writer.write(observation, XMLFILE)
    #************************************************************************
    # Apply archive-specific changes to a plane in an observation xml
    #************************************************************************
    def build_plane_custom(self, 
                           xmlfile, 
                           collection,
                           observationID,
                           productID):
        """
        For the observation described in xmlfile and for the requested plane
        apply archive-specific modifications.  This can use metadata stored 
        in the plane "custom" dictionary.

        Arguments:
        xmlfile: path to the xmlfile that needs to be modified
        collection: collection to edit
        observationID: the observationID to edit
        productID: the productID to edit
        """
        pass
#        observation = self.reader.read(xmlfile)
#        change = False
#        <implement the changes>
#        if change:
#            with open(xmlfile, 'w') as XMLFILE:
#                self.writer.write(observation, XMLFILE)

    #************************************************************************
    # Apply archive-specific changes to a plane in an observation xml
    #************************************************************************
    def build_fitsuri_custom(self, 
                             xmlfile, 
                             collection,
                             observationID,
                             productID, 
                             fitsuridict):
        """
        For the observation described in xmlfile and for the requested plane
        and artifact/part specified by fitsuri, apply archive-specific
        modifications.  This can use metadata stored in the fitsuri "custom"
        dictionary.

        Arguments:
        xmlfile: path to the xmlfile that needs to be modified
        collection: collection to edit
        observationID: the observationID to edit
        productID: the productID to edit
        """
        pass
#        observation = self.reader.read(xmlfile)
#        change = False
#        <implement the changes>
#        if change:
#            with open(xmlfile, 'w') as XMLFILE:
#                self.writer.write(observation, XMLFILE)

    def __init__(self):
        """
        Initialize the ingest2caom2 structure, especially the attributes
        storing default values for command line switches.

        Arguments:
        <none>

        It is normally necessary to override __init__ in a derived class,
        supplying archive-specific values for some of the fields, e.g.
            def __init__(self):
                ingest2caom2.__init__(self)
                self.archive  = <myarchive>
                self.database = <mydatabase>
                self.stream   = <mystream>

        The attribute local_args specifies additional switches that will be
        applied to every instance of fits2caom2.  Possible items include:
            --cert=<cert file>
            --key=<unencrypted key file>
            --test
        The method runFits2caom2 also has an argument args that
        allows these switches to be inserted in the fits2caom2 command
        dynamically.

        Derived classes will probably want to use their own versions of
        filterfunc and either gtfunc or cmpfunc.

        The filterfunc is a name checking function that returns True if
        a filename is valid for ingestion and False otherwise.
        The signature of filterfunc is filterfunc(filename), i.e. it
        operates on filenames rather than file_id's and can use the
        file extension to help determine if the file name is valid.
        
        Note that filterfunc, gtfunc and cmpfunc are functions, not class 
        methods.  The filter functions supplied with ingest2caom2 are:

            fitsfilter : accept only files with extensions ['.fits', '.fit']
            nofilter : accept everything

        The default is fitsfilter.

        The gtfunc and cmpfunc implement file_id comparisons for sorting.
        The implementation of gtfunc(fid1, fid2) should return True if fid1
        should come after fid2 in sorted order.  Similarly,
        cmpfunc(fid1, fid2) is a full comparison function that returns 1 if
        fid1 belongs after fid2, 0 if the order does not matter, and -1
        if fid1 belongs before fid2.

        By default cmpfunc is implemented using gtfunc, because it is often
        easier to write a one-sided comparison,  However, only cmpfunc is used
        in the ingest2caom2 code, so in some cases it may be useful to bypass
        gtfunc by redefining self.cmpfunc. Only one comparison function is 
        supplied with ingest2caom2, which is thus the default:

            nosort : do not sort the files
        """
        # default path to configuration files, computed relative to the path
        # to the executable.
        self.configpath = os.path.abspath(os.path.dirname(sys.argv[0]) +
                                          '/../config')
        # config object optionally contains a user configuration object
        # this can be left undefined at the CADC, but is needed at other sites
        self.userconfig = {}
        self.userconfigpath = None

        # -------------------------------------------
        # placeholders for command line switch values
        # -------------------------------------------
        # Command line options
        self.arg = argparse.ArgumentParser('ingest2caom2')
        self.switches = None
        
        # If true, gather metadata and resubmit as a set of gridengine jobs
        self.qsub = None
        self.queue = 'cadcproc'
        self.big = False
        self.gridengine = None

        # Current container
        self.container = None

        # routine to convert file basenames into file_ids
        self.make_file_id = make_file_id

        # archive and database access parameters
        self.archive = None
        self.stream = None
        self.adput = False

        self.server = None
        self.database = 'dummy'
        self.schema = None
        self.user = None # used to get credentials for connection

        # fits2caom2 configuration files
        # Derived classes can use self.configpath to find appropriate
        # config and default files.
        self.config = None
        self.default = None

        # files and directories
        self.infiles = None
        self.outdir = None

        # log handling
        self.logfile = None
        self.keeplog = False
        self.log = None
        self.loglevel = logging.INFO
        self.test = None
        self.debug = False

        # The filterfunc is a name checking function that returns True if
        # a filename is valid for ingestion and False otherwise.
        # The signature of filterfunc is filterfunc(filename), i.e. it
        # operates on filenames rather than file_id's and can use the
        # file extension to help determine if the file name is valid.
        # Note that gtfunc is a function, not a class method.

        # By default, ingest only FITS files.
        self.filterfunc = fitsfilter

        # The gtfunc and cmpfunc implement file_id comparisons for sorting.
        # The implementation of gtfunc(fid1, fid2) should return True if fid1
        # should come after fid2 in sorted order.  Similarly,
        # cmpfunc(fid1, fid2) is a full comparison function that returns 1 if
        # fid1 belongs after fid2, 0 if the order does not matter, and -1
        # if fid1 belongs before fid2.  By default cmpfunc is implemented using
        # gtfunc, because it is usually easier to write a one-sided comparison,
        # However, only cmpfunc is used in the ingest2caom2 code, so in some
        # cases it may be useful to bypass gtfunc by redefining self.cmpfunc.
        # Note that these are NOT methods of the class.

        # By default do not sort the file_id's
        self.gtfunc = nosort
        self.cmpfunc = lambda f1, f2: 1 if self.gtfunc(f1, f2) else \
                                     -1 if self.gtfunc(f2, f1) else 0

        # local_args - configure for archive specific operations
        self.local_args = ''

        # The metadata dictionary - fundamental structure for the entire class
        # For the detailed structure of metadict, see the help text for
        # fillMetadictFromFile()
        self.metadict = OrderedDict()

        # The plane and fileuri dictionaries are local storage used to build
        # the actual plane and artifact dictionaries, and will be overwritten
        # regularly.  They are managed using the methods
        #   add_to_plane_dict(key, value)
        #   add_to_fitsuri_dict(key, value)
        #   clear_dicts()
        self.file_id = ''
        self.uri= ''
        self.collection = None
        self.observationID = None
        self.productID = None
        self.plane_dict = OrderedDict()
        self.fitsuri_dict = OrderedDict()
        self.override_items = 0
        
        # local sets to be accumulated in a plane
        self.memberset = set([])
        self.inputset = set([])

        # list of containers for input files
        self.containerlist = []

        # set console_output = False for quiet operation, useful in testing
        self.console_output = True

        # to use the pyCAOM2 library, initialize the reader and writer
        self.reader = ObservationReader(True)
        self.writer = ObservationWriter()
        
        # storage for the optional connection to the database
        self.conn = None

    #************************************************************************
    # Clear the local plane and artifact dictionaries
    #************************************************************************
    def clear(self):
        """
        Clear the local plane and artifact dictionaries.

        Arguments:
        <none>
        """
        self.file_id = ''
        self.uri = ''
        self.collection = None
        self.observationID = None
        self.productID = None
        self.plane_dict.clear()
        self.fitsuri_dict.clear()
        self.memberset.clear()
        self.inputset.clear()
        self.override_items = 0

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

    #************************************************************************
    # Non-fatal responses to errors encountered in files
    # Inside build_dict, calling ignore_artifact or reject_plane and
    # immediately returning will cause the artifact or plane to be
    # skipped  during ingestion.
    #************************************************************************
    def ignore_artifact(self, message):
        """
        Ignore this artifact due to an error encountered while processing
        the metadata from the current file.

        Arguments:
        message : an explanatory message that will be logged to the console
        """
        self.log.console(self.file_id +
                         ': the following condition prevented the'
                         ' artifact from being ingested:' +
                         message,
                         loglevel=logging.WARNING)

        self.clear()

    def reject_plane(self, message):
        """
        Reject the current plane due to an error encountered while processing
        the metadata from the current file.  Processing will continue, but
        the presence of ingest2caom2.REJECT in the plane_dict will prevent
        the override file from being written or fits2caom2 from being called.

        Arguments:
        message : an explanatory message that will be logged to the console
        """
        self.log.console(self.file_id +
                         ': the following condition prevented the'
                         ' entire plane from being ingested:' +
                         message,
                         loglevel=logging.WARNING)

        self.add_to_plane_dict(ingest2caom2.REJECT, ingest2caom2.REJECT)

    #************************************************************************
    # Define the standardcommand line interface.
    # Be sure to maintain consistency amongst defineCommandLineSwitches,
    # processCommandLineSwitches, and logCommandLineSwitches.
    #************************************************************************
    def defineCommandLineSwitches(self):
        """
        Generic routine to build the standard list of command line switches.
        This routine has been split off from processing and logging to allow
        additional switches to be defined for derived classes.
        
        Subclasses for specific archive can override this method to add new
        switches, but should first call 
           self.ingest2caom2.defineCommandLineSwitches()
        to ensure that the standard switches are always defined.

        Arguments:
        <none>

        # gridengine switch
        --qsub     :  (optional) submit to gridengine (see below)
        --queue    : (optional) gridengine queue for jobs

        # archive and database switches
        --archive   : default normally set in __init__
        --stream'   : ad stream to use with adPut (only needed with --adput)

        --adput     : use adPut to push files into ad before ingestion

        --server    : choices=['SYBASE','DEVSYBASE'], default='SYBASE'
        --database  : default normally set in __init__
        --schema    : default='dbo'

        # fits2caom2 options
        --config    : (optional) path to fits2caom2 config file
        --default   : (optional) path to fits2caom2 default file

        # File and directory options
        --outdir    : output (working) directory (default = current directory)

        # debugging options
        --log       : (optional) name of the log file
        --keeplog   : (optional) keep log if successful (default is to delete)
        --test      : (optional) simulate operation of fits2caom2
        --quiet     : (optional) suppress warning, info and debug messages
                      (default is to suppress info and debug messages)
        --verbose   : (optional) suppress only debug messages
        --debug     : (optional) pass all messages and retain xml and 
                      override files

        Any additional arguments are interpreted as a list of files and
        containers to ingest.  A directory is interpretted as a 
        filelist_container including every file in the directory.  A tar file
        (optionally gzipped) is interpreted as a tarfile_container including
        every file in the tar file.  A URI of the form ad:<path_to_text_file>
        will be interpreted as an adfile_container, with the expectation that 
        the file contains a list of adURIs, one per line.  A URI of the form
        dp:<number> will be interpreted as a dataproc_container, fetching
        the list of adURIs from the data_proc:dp_recipe_output table using
        identity_instance_id=<number> as the key in the table.  Any other 
        files will be added to a filelist_container.

        If --qsub is specified, a separate gridengine job will be started for
        each container.  Thus,
            ingest2caom2 --qsub A.tar ad:B.ad C.fits D.fits
        would start 3 gridengine jobs, one for A.tar, one for B.ad, and one
        for the two fits files C.fits and D.fits.

        If there is a single container, the log file name defaults to the
        name of the container, massaged to remove non-standard characters. 
        If not otherwise specified, the name of the log file defaults to
        <database>.log.  

        Beware that log files are always opened in append.  Be sure to delete
        any existing log files if it is important to have a clean record of the
        current ingestion.
        """

        # Optional user configuration
        if self.userconfigpath:
            self.arg.add_argument('--userconfig',
                                  default=self.userconfigpath,
                                  help='Optional user configuration file '
                                  '(default=' + self.userconfigpath + ')')

        # Grid Engine options
        self.arg.add_argument('--qsub',
            action='store_true',
            help='(optional) submit to gridengine')
        self.arg.add_argument('--queue',
            default='cadcproc',
            help='gridengine queue to use if --qsub is set')

        # Basic fits2caom2 options
        # --archive and --stream will normally be assigned default values
        #     in __init__
        # --archive is needed with adput to store files in ad
        self.arg.add_argument('--archive',
            help='mandatory AD archive recording the data files')
        #  --stream is needed with --adput to ingest files into ad
        self.arg.add_argument('--stream',
            help='(optional) use this stream with adPut')
        self.arg.add_argument('--adput',
            action='store_true',
            help='(optional) use adPut to put FITS files into AD')

        self.arg.add_argument('--server',
            default='SYBASE',
            choices=['SYBASE', 'DEVSYBASE'])
        self.arg.add_argument('--database')
        self.arg.add_argument('--schema',
            default='dbo')

        # Optionally, specify explicit paths to the config and default files
        self.arg.add_argument('--config',
            help='(optional) path to fits2caom2 config file')
        self.arg.add_argument('--default',
            help='(optional) path to fits2caom2 default file')

        # Big jobs require extra memory
        self.arg.add_argument('--big',
            action='store_true',
            help='(optional) request extra heap space and RAM')
        
        # output directory
        self.arg.add_argument('--outdir',
            default='.',
            help='output directory, (default = current directory')

        # debugging options
        self.arg.add_argument('--keeplog',
            action='store_true',
            help='(optional) keep log if successful (default is to delete)')
        self.arg.add_argument('--test',
            action='store_true',
            help='(optional) simulate operation of fits2caom2')
        self.arg.add_argument('--logdir',
                        help='(optional) directory to hold log file')
        self.arg.add_argument('--log',
                        help='(optional) name of the log file')
        self.arg.add_argument('--quiet',
            action='store_const',
            dest='loglevel',
            const=logging.WARN,
            help='(optional) only show warning and error messages')
        self.arg.add_argument('--verbose',
            action='store_const',
            dest='loglevel',
            const=logging.DEBUG,
            help='(optional) show all messages')
        self.arg.add_argument('--debug',
            action='store_true',
            help='(optional) show all messages, pass --debug to fits2caom2,'
            ' and retain all xml and override files')

        self.arg.add_argument('input',
            nargs='*',
            help='file(s) or container(s) to ingest')

    #************************************************************************
    # Process the command line interface.
    # Be sure to maintain consistency amongst defineCommandLineSwitches,
    # processCommandLineSwitches, and logCommandLineSwitches.
    #************************************************************************
    def processCommandLineSwitches(self):
        """
        Generic routine to process the command line switches
        and create outdir if necessary.  This will check the values of the
        standard switches defined in defineCommandLineSwitches and will
        leave the additional switches in self.switches.
        
        Arguments:
        <None>
        
        Returns:
        The set of command line switches is stored in self.switches and the
        default switches are interpreted and stored into individual attributes.
        """

        self.switches = self.arg.parse_args()

        # If the user configuration file exists, read it
        # Regardless of whether the file exists, after this point
        # the self.userconfig dictionary exists.
        if 'userconfig' in self.switches:
            self.userconfigpath = os.path.abspath(
                                    os.path.expanduser(
                                        os.path.expandvars(
                                            self.switches.userconfig)))
        if self.userconfigpath:
            self.read_user_config(self.userconfigpath)
            
        self.userconfig['server'] = self.switches.server

        # For finer control, set values for database tables in the 
        # user configuration file
        if self.switches.database:
            self.database = self.switches.database
            self.userconfig['cred_db'] = self.database

        # Save the values in self
        # A value on the command line overrides a default set in code.
        # Options with defaults are always defined by the command line.
        # It is not necessary to check for their existance.
        if self.switches.qsub:
            self.qsub = self.switches.qsub

        if self.switches.archive:
            self.archive = self.switches.archive
        if self.switches.stream:
            self.stream = self.switches.stream
        self.adput = self.switches.adput

        self.schema = self.switches.schema

        if self.switches.big:
            self.big = self.switches.big
        
        if self.switches.config:
            self.config = os.path.abspath(self.switches.config)
        if self.switches.default:
            self.default = os.path.abspath(self.switches.default)

        self.outdir = os.path.abspath(
                         os.path.expandvars(
                             os.path.expanduser(self.switches.outdir)))
        
        if self.switches.logdir:
            self.logdir = os.path.abspath(
                            os.path.expandvars(
                                os.path.expanduser(self.switches.logdir)))
        else:
            self.logdir = self.outdir

        self.test = self.switches.test

        if self.switches.loglevel:
            self.loglevel = self.switches.loglevel

        # As the help message indicated, --debug overrides --quiet and --verbose
        if self.switches.debug:
            self.loglevel = logging.DEBUG
            self.debug = True

        logbase = self.database
        if len(self.switches.input) == 1:
            logbase = re.sub(r'[^a-zA-Z0-9]', r'_', 
                             os.path.splitext(
                                 os.path.basename(
                                     self.switches.input[0]))[0])
        logbase += '_'

        # log file name
        # If the log file already exists, do not delete it on successful exit.
        # Otherwise, the default behaviour will be to delete a log file that
        # is created for this program on successful exit.
        if self.switches.log:
            if os.path.dirname(self.switches.log) == '':
                self.logfile = os.path.join(self.logdir, self.switches.log)
            else:
                self.logfile = os.path.abspath(self.switches.log)
        
        self.keeplog = self.switches.keeplog
        if self.logfile:
            if os.path.exists(self.logfile):
                self.keeplog = True
        else:
            # make a log file that is temporary unless the ingestion fails
            self.logfile = os.path.join(self.logdir,
                                        logbase + '_' + utdate_string() + 
                                        '.log')
        
        # check for consistency and correctness of switches
        if not self.archive:
            raise RuntimeError('--archive is required')

        if not self.database:
            raise RuntimeError('--database is required')

        if self.adput and not self.stream:
            raise RuntimeError('--stream must be defined (possibly as '
                               'a default) when --adput is used')

        # create outdir if it does not already exist
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

        if not os.path.isdir(self.outdir):
            raise RuntimeError('outdir is not a directory: ' + self.outdir)

        if not self.config:
            self.config = os.path.join(self.configpath,
                                       self.database + '.config')

        if not self.default:
            self.default = os.path.join(self.configpath,
                                         self.database + '.default')

        if len(self.switches.input) == 0:
            raise RuntimeError('no input files or containers to ingest')
        self.inputlist = self.switches.input
        
    #************************************************************************
    # Parse command line containers from input list
    #************************************************************************
    def commandLineContainers(self):
        """
        Generic routine to process the list of inputs from the command
        line.

        Arguments:
        <None>

        The list of inputs is passed through the attribute self.inputlist and
        is a list of files, directories and dp:uri's to ingest:
        - a directory will be expanded into a filelist_container
        - a tar file (optionally gzipped) will be read into a tarfile_container
          from which each file will be untarred into outdir and ingested.
        - a text file with the extension ".ad" will be parsed for
          AD URI's, one per line, from which an adfile_container will be
          constructed.
        - an identity_instance_id in dp_recipe_output in the format 
          dp:identity_instance_id will be used as a key into the table to
          read a list of AD URI's.
        - any other files will be added to the default filelist_container.

        If --qsub is specified, a separate gridengine job will be started for
        each container.  Thus,
            ingest2caom2 --qsub A.tar B.ad C.fits D.fits
        would start 3 gridengine jobs, one for A.tar, one for B.ad, and one
        for the two fits files C.fits and D.fits.

        If not specified, the name of the log file defaults to <database>.log.
        Beware that log files are always opened in append.  Be sure to delete
        any existing log files if it is important to have a clean record of the
        current run.
        """

        # Find the lists of containers and files to ingest.
        # If a container has been specified, other files are ignored
        self.containerlist = []
        if not self.inputlist:
            self.log.console('No files or directories were supplied'
                             ' as inputs', 
                             logging.WARN)

        else:
            try:
                argfilelist = []
                for f in self.inputlist:
                    service = 'file'
                    key = ''
                    m = re.match(r'^([a-z0-9]+):([\S]+$)', f)
                    if m:
                        service, key = m.group(1, 2)
                        
                        if service == 'ad':
                            absf = os.path.abspath(
                                       os.path.expandvars(
                                           os.path.expanduser(key)))
                            self.log.console('ad container: ' + absf,
                                             logging.DEBUG)
                            if self.qsub:
                                self.submitJobToGridEngine('ad:' + absf)
                            else:
                                self.containerlist.append(
                                    adfile_container(absf,
                                                     self.outdir,
                                                     self.filterfunc))

                        elif service == 'dp':
                            self.log.console('dp container: ' + key,
                                             logging.DEBUG)
                            if self.qsub:
                                self.submitJobToGridEngine(f)
                            else:
                                self.containerlist.append(
                                    dataproc_container(key,
                                                 self.conn,
                                                 self.outdir,
                                                 self.filterfunc))
                        
                        else:
                            self.log.console('unknown service: "' + f + '"',
                                             logging.ERROR)
                    else:
                        absf = os.path.abspath(
                                   os.path.expandvars(
                                       os.path.expanduser(f)))
                        if os.path.exists(absf):
                            if os.path.isdir(absf):
                                self.log.console('dir container: ' + absf,
                                                 logging.DEBUG)
                                if self.qsub:
                                    self.submitJobToGridEngine(absf)
                                else:
                                    dirlist = [os.path.join(absf, ff)
                                               for ff in os.listdir(absf)]
                                    self.containerlist.append(
                                        filelist_container(
                                            os.path.basename(absf),
                                            dirlist,
                                            self.filterfunc,
                                            self.make_file_id))
                            elif tarfile.is_tarfile(absf):
                                self.log.console('tar container: ' + absf,
                                                 logging.DEBUG)
                                if self.qsub:
                                    self.submitJobToGridEngine(absf)
                                else:
                                    self.containerlist.append(
                                        tarfile_container(absf,
                                                          self.outdir,
                                                          self.filterfunc,
                                                          self.make_file_id))
                            else:
                                argfilelist.append(absf)
                        else:
                            self.log.console('The input does not exist: ' + f,
                                             logging.WARN)

                if argfilelist:
                    self.log.console('file container: ' + ','.join(argfilelist),
                                     logging.DEBUG)
                    if self.qsub:
                        self.submitJobToGridEngine(argfilelist)
                    else:
                        self.containerlist.append(
                            filelist_container('filelist',
                                               argfilelist,
                                               self.filterfunc,
                                               self.make_file_id))
            except Exception as e:
                msg = '\n'.join([traceback.format_exc(), str(e)])
                self.log.console(msg,
                                 logging.ERROR)
            

    #************************************************************************
    # Log the values of the command line switches
    # Be sure to maintain consistency amongst defineCommandLineSwitches,
    # processCommandLineSwitches, and logCommandLineSwitches.
    #************************************************************************
    def logCommandLineSwitches(self):
        """
        Generic method to start the logger and
        log the command line switch values

        Arguments:
        <none>
        """
        if not self.log:
            # Do not re-open the log if it has already been done
            self.log = logger(self.logfile,
                              self.loglevel,
                              console_output=self.console_output)

        # Report switch values
        self.log.file('tools4caom2version = ' + __version__.version)
        self.log.file('configpath         = ' + str(self.configpath))
        self.log.file('qsub               = ' + str(self.qsub))
        self.log.file('queue              = ' + self.queue)
        self.log.file('big                = ' + str(self.big))
        self.log.file('')
        self.log.file('archive            = ' + str(self.archive))
        self.log.file('stream             = ' + str(self.stream))
        self.log.file('adput              = ' + str(str(self.adput)))
        self.log.file('')
        self.log.file('server             = ' + str(self.server))
        self.log.file('database           = ' + str(self.database))
        self.log.file('schema             = ' + str(self.schema))
        self.log.file('')
        self.log.file('config             = ' + str(self.config))
        self.log.file('default            = ' + str(self.default))
        self.log.file('')
        self.log.file('outdir             = ' + str(self.outdir))
        self.log.file('logfile            = ' + str(self.logfile))
        self.log.file('test               = ' + str(self.test))
        self.log.file('debug              = ' + str(self.debug))

        self.log.file('')

    #************************************************************************
    # Submit a single job to gridengine
    #************************************************************************
    def submitJobToGridEngine(self, container):
        """
        Generic method to submit a job to gridengine to ingest the specified 
        container.

        Arguments:
        container  : a container or list of files to ingest
        """
        # Generate a name for the shell script in the same directory as the log
        # If a single container name is supplied, use it as the base for
        # the shelscript and log file names.  Otherwise, use the current log
        # file name.  Pad these names with the current UTC time.
        # Note that a new file is generated each time and for each container
        # Delete them when no longer needed.
        if not self.gridengine:
            if self.big:
                self.gridengine = gridengine(
                                   self.log,
                                   queue=self.queue,
                                   options='-cwd -j yes -l cmem=32')
            else:
                self.gridengine = gridengine(self.log, 
                                         queue=self.queue)


        cshdir = os.path.abspath(os.path.dirname(self.logfile))
        suffix = re.sub(r':', 
                        '-',
                        '_' + datetime.datetime.utcnow().isoformat())
        # containerfile = os.path.basename(cshfile[1]) + '.pickle'
        
        if isinstance(container, str):
            # if the container is a URI, pick out the real container
            if re.search(r':', container):
                service, identifier = re.split(r':', container)
                
                if service == 'dp':
                    containername = 'dp-' + identifier
                else:
                    containername = os.path.splitext(
                        os.path.basename(identifier))[0]
                containerpath = container
            else:
                containername = os.path.splitext(
                    os.path.basename(container))[0]
                containerpath = os.path.abspath(
                    os.path.expanduser(os.path.expandvars(container)))
        else:
            containername = 'filelist'
            containerpath = ' '.join([os.path.abspath(
                os.path.expanduser(os.path.expandvars(f))) for f in container])

        cshfile = os.path.join(cshdir, containername + suffix + '.csh')
        containerlog = os.path.join(cshdir, containername + suffix + '.log')

        cmd = os.path.abspath(sys.argv[0])

        cmd += ' --archive=' + self.archive
        if self.stream:
            cmd += ' --stream=' + self.stream
        if self.adput:
            cmd += ' --adput'

        if self.server:
            cmd += ' --server=' + self.server
        if self.database:
            cmd += ' --database=' + self.database
        if self.schema:
            cmd += ' --schema=' + self.schema
        if self.user:
            cmd += ' --user=' + self.user

        if self.config:
            cmd += '  --config=' + self.config
        if self.config:
            cmd += '  --default=' + self.default

        cmd += ' --outdir=$TMPDIR'

        if self.test:
            cmd += ' --test'

        cmd += ' --log=' + containerlog
        if self.loglevel == logging.ERROR:
            cmd += ' --quiet'
        elif self.loglevel == logging.INFO:
            cmd += ' --verbose'
        elif self.loglevel == logging.DEBUG:
            cmd += ' --debug'

        cmd += ' ' + (containerpath)

        if self.test:
            status = 0
            output = ''
        else:
            self.gridengine(cmd, cshfile, containerlog)

        # these file names can be discarded, but are useful for test purposes
        return (cshfile, containerpath)

    #************************************************************************
    # Read FITS headers and fill the metadict structure.
    #************************************************************************
    def verifyFileInAD(self, filename):
        """
        Generic method to check whether a specified file is in AD.
        If adput has been requested, first try to put filename into AD.
        
        NB: We do not need to know that a file is in AD in order to ingest it.
        If adput is not requested, assume that the file is already in AD.
        

        Arguments:
        filename : path to the file on disk
        """
        if not self.adput:
            return
        
        self.log.file('verifyFileInAD ' + filename,
                      logging.DEBUG)

        #*****************************************************************
        # Put the file into AD if so requested
        #*****************************************************************
        file_id = os.path.basename(os.path.splitext(filename)[0])
        if self.archive and self.stream:
            cmd = 'adPut -a %s -as %s -replace %s' % (self.archive,
                                                     self.stream,
                                                     filename)
            output = ''
            if self.test:
                self.log.console('TEST: ' + cmd)
                status = 0
            else:
                self.log.file(cmd)
                status, output = commands.getstatusoutput(cmd)

            if status == None or status != 0:
                self.log.console('adPut of %s failed with status %s\n' %
                               (filename, status) + output,
                               logging.ERROR)

        #*****************************************************************
        # Verify that the file is in AD
        #*****************************************************************
        cmd = 'adInfo -a %s -s %s' % (self.archive, file_id)
        if self.test:
            self.log.console('TEST: ' + cmd)
            status = 0
        else:
            self.log.file(cmd)
            # work-around for adInfo bug - retry up to 3 times with
            # increasing delays
            numtries = 0
            status = 0
            while numtries < 3 and status:
                status, adfilename = commands.getstatusoutput(cmd)
                if status:
                    numtries += 1
                    self.log.file('retry adInfo %s: %d' % (file_id, numtries),
                                  logging.WARNING)
                    time.sleep(1.0 * numtries)

            if status == None or status != 0:
                self.log.file('adInfo of %s failed with status %s\n' %
                                (filename, status) + output,
                          logging.ERROR)

    def fillMetadictFromFile(self, file_id, filepath, local):
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
            head = pyfits.getheader(filepath)
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

    #************************************************************************
    # Fill metadict using metadata from each file
    #************************************************************************
    def fillMetadict(self, container):
        """
        Generic routine to filll the metadict structure by iterating over
        all containers (directories, tarfiles, adfiles, and file lists),
        generating for each container a filtered and sorted list of
        (file_id, filename) pairs and extracting the required metadata
        from each file in turn using fillMetadictFromFile().

        Arguments:
        <none>
        """
        try:
            local = False
            if isinstance(container, filelist_container):
                local = True
            
            # sort the file_id_list
            file_id_list = sorted(container.file_id_list(), 
                                  cmp=self.cmpfunc)
            self.log.file('in fillMetadict, file_id_list = ' +
                          repr(file_id_list),
                          logging.DEBUG)

            # verify that each file is in ad and ingest its metadata
            for file_id in file_id_list:
                self.log.file('In fillMetadict, use ' + file_id,
                              logging.DEBUG)
                with container.use(file_id) as f:
                    # Note that this is the first place we see
                    # the full file name
                    if isinstance(container, adfile_container):
                        self.fillMetadictFromFile(file_id, f, local)
                    elif self.filterfunc(f):
                        self.verifyFileInAD(f)
                        self.fillMetadictFromFile(file_id, f, local)
        finally:
            container.close()

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
        filepath = os.path.join(self.outdir,
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
        if localstring:
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
            status, output = commands.getstatusoutput(cmd)

            # if the first attempt to run fits2caom2 fails, try again with
            # --debug to capture the full error message
            if status:
                self.log.console("fits2caom2 return status %d" % (status))
                if not debug:
                    self.log.console("fits2caom2 - rerun in debug mode")
                    cmd += ' --debug'
                    status, output = commands.getstatusoutput(cmd)
                self.log.console("output = '%s'" % (output), 
                                 logging.ERROR)
            elif debug:
                self.log.file("output = '%s'" % (output))

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
        repository = Repository(self.outdir, 
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

                            if ingest2caom2.REJECT in thisPlane['plane_dict']:
                                break

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

                            self.runFits2caom2(collection,
                                               observationID,
                                               productID,
                                               xmlfile,
                                               override,
                                               uristring,
                                               localstring,
                                               arg=arg,
                                               debug=self.switches.debug)
                            self.log.file('SUCCESS: observationID=%s '
                                          'productID="%s"' %
                                                (observationID, productID))
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
    # if not an interactive shell, rescue the xml files by copy to the log dir
    #************************************************************************
    def rescue_xml(self):
        """
        if log directory and outdir are different copy all xml files to log dir
        
        Arguements:
        <none>
        """
        logdir = os.path.dirname(os.path.abspath(self.logfile))
        outdir = os.path.abspath(self.outdir)
        if logdir != outdir:
            xmllist = [(os.path.join(outdir, f),
                        os.path.join(logdir, f)) for f in os.listdir(outdir)
                       if os.path.splitext(f)[1] in ['.xml', '.override']]
            for src, dst in xmllist:
                shutil.copyfile(src, dst)
            
        
        
    #************************************************************************
    # Run the program
    #************************************************************************
    def run(self):
        """
        Generic method to run the ingestion, either by submitting a set of
        jobs to GridEngine, or by running the ingestions locally

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
            if self.qsub:
                self.commandLineContainers()
            else:
                # It is harmless to create a database connection object if it
                # is not going to be used, since the actual connections use
                # lazy initialization and are not opened until a call to read 
                # or write is made.
                with connection(self.userconfig,
                                self.log) as self.conn:
                    self.commandLineContainers()
                    for c in self.containerlist:
                        self.log.console('PROGRESS: container = ' + c.name)
                        self.fillMetadict(c)
                        try:
                            self.ingestPlanesFromMetadict()
                        except:
                            self.rescue_xml()
                            raise
            # if no errors, declare we are DONR
            self.log.console('DONE')
        
        # if execution reaches here, the ingestion was successful, 
        # so, if not in debug mode, delete the log file
        if self.loglevel != logging.DEBUG and not self.keeplog:
            os.remove(self.logfile)

if __name__ == '__main__':
    myingest2caom2 = ingest2caom2()
    myingest2caom2.run()
