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
#*   Script Name:    testIngest2caom2.py
#*
#*   Purpose:
#+    Unit test module for tools4caom2.ingest2caom2.
#*
#*   Classes:
#*
#*   Functions:
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
__author__ = "Russell O. Redman"
__version__ = "1.0"


import cPickle
from collections import OrderedDict
import commands
from datetime import datetime
import filecmp
import logging
import numpy
import os
import os.path
import pyfits
import re
import string
import sys
import tarfile
import tempfile
import unittest

from tools4caom2.ingest2caom2 import ingest2caom2
from tools4caom2.adfile_container import adfile_container
from tools4caom2.filelist_container import filelist_container
from tools4caom2.tarfile_container import tarfile_container
from tools4caom2.ingest2caom2 import make_file_id
from tools4caom2.ingest2caom2 import nofilter
from tools4caom2.logger import logger

from caom2.caom2_observation_uri import ObservationURI
from caom2.caom2_plane_uri import PlaneURI

# Is the archive accessible on the current platform?
status, output = commands.getstatusoutput('which adPut')
if status:
    adput_available = False
else:
    adput_available = True

def write_fits(filepath,
               numexts,
               obsid,
               product,
               member=None,
               provenance=None):
    """
    Write a FITS test file with the requested PRODUCT keyword and number
    of extensions.

    Arguments:
    filepath  : path to the new file
    numexts   : number of extensions
    product   : product type

    In this example, inputs and provenance will be recorded using the file_id
    of the input file.
    """
    data = numpy.arange(10)
    datestring = datetime.utcnow().isoformat()
    hdu = pyfits.PrimaryHDU(data)
    # parse the filepath
    filebase = os.path.basename(filepath)
    file_id, ext = os.path.splitext(filebase)
    hdu.header.update('FILE-ID', file_id)
    hdu.header.update('COLLECT', 'TEST')
    hdu.header.update('OBSID', obsid)

    # DPDATE will be different every time the program runs, so it should be
    # possible to verify that the files have been updated in AD by checking
    # this header.
    hdu.header.update('DPDATE', datestring)
    hdu.header.update('PRODUCT', product)
    hdu.header.update('NUMEXTS', numexts)
    hdu.header.update('FIELD1', 'F1%s' % (product))
    hdu.header.update('FIELD2', 'F2%s' % (product))

    # Some product-dependent headers
    if product != 'A':
        hdu.header.update('FIELD3', 'F3%s' % (product))
        hdu.header.update('NOTA', True)
    else:
        hdu.header.update('NOTA', False)

    #Some extension-dependent headers
    hdu.header.update('FIELD4', 'BAD')
    hdu.header.update('FIELD5', 'GOOD')

    # Composite products have members identified by their file_id's
    if isinstance(member, list):
        hdu.header.update('OBSCNT', len(member))
        for i, name in enumerate(member):
            hdu.header.update('OBS%d' % (i + 1), name)
    elif isinstance(member, str):
        hdu.header.update('OBSCNT', '1')
        hdu.header.update('OBS1', member)

    # Derived products have inputs identified by their file_id's
    if isinstance(provenance, list):
        hdu.header.update('PRVCNT', len(provenance))
        for i, name in enumerate(provenance):
            hdu.header.update('PRV%d' % (i + 1), name)
    elif isinstance(provenance, str):
        hdu.header.update('PRVCNT', '1')
        hdu.header.update('PRV1', provenance)

    hdulist = pyfits.HDUList(hdu)

    # Optionally add extensions
    for extension in range(1, numexts + 1):
        hdu = pyfits.ImageHDU(data)
        hdu.header.update('EXTNAME', 'EXTENSION%d' % (extension))
        hdu.header.update('OBSID', obsid)
        hdu.header.update('PRODUCT', '%s%d' % (product, extension))
        hdu.header.update('DPDATE', datestring)
        hdu.header.update('FIELD1', 'F1%s%d' % (product, extension))
        hdu.header.update('FIELD2', 'F2%s%d' % (product, extension))

        # Product dependent headers
        if product != 'A':
            hdu.header.update('FIELD3', 'F3%s' % (product))
            hdu.header.update('NOTA', True)
        else:
            hdu.header.update('NOTA', False)

        # Extension-dependent headers
        hdu.header.update('FIELD4', 'GOOD')
        hdu.header.update('FIELD5', 'BAD')
        # an extension-specific header
        hdu.header.update('HEADER%d' % (extension),
                          'H%s%d' % (product, extension))

        hdulist.append(hdu)

    hdulist.writeto(filepath)

class testIngest2caom2(unittest.TestCase):
    """
    unit tests for tools4caom2.ingest2caom2 classes
    """
    # A specialization for testing purposes
    class TestIngest(ingest2caom2):
        def __init__(self):
            ingest2caom2.__init__(self)
            self.make_file_id = make_file_id

        def build_dict(self, header):
            file_id = header['file_id']

            # is this a catalog of type D?
            if string.find(file_id, '_') > 0:
                # Yes; get metadata from previously ingested FITS file
                fits_file_id = re.split('_', file_id)[0]
                input_fitsURI = self.fitsfileURI(self.archive,
                                                 fits_file_id,
                                                 fits2caom2=False)
                (c, o, p) = self.findURI(input_fitsURI)

                collection = c
                observationID = o
                productID = 'D'
                field1 = self.get_plane_value(c, o, p, 'field1')
                field2 = self.get_plane_value(c, o, p, 'field2')
                field3 = 'CATALOG'

                self.collection = collection
                self.observationID = observationID
                self.productID = productID
                self.add_to_plane_dict('field1', field1)
                self.add_to_plane_dict('field2', field2)
                self.add_to_plane_dict('field3', field3)
                self.planeURI(c, o, p)
                artifact_uri = self.fitsfileURI('TEST', file_id)

            else:
                # No, this is a FITS file
                collection = header['COLLECT']
                observationID = header['OBSID']
                productID = header['PRODUCT']
                self.productID = productID
                self.collection = collection
                self.observationID = observationID

                self.add_to_plane_dict('field1', header['FIELD1'])
                self.add_to_plane_dict('field2', header['FIELD2'])
                if header['PRODUCT'] == 'A':
                    self.add_to_plane_dict('field3', 'NOVALUE')
                else:
                    self.add_to_plane_dict('field3', header['FIELD3'])

                if 'OBSCNT' in header:
                    obscnt = int(header['OBSCNT'])
                    for i in range(1, obscnt + 1):
                        obsn = 'OBS%d' % (i)
                        if obsn in header:
                            fileURI = self.fitsfileURI(self.archive,
                                                       header[obsn],
                                                       fits2caom2=False)
                            (co, ob, pl) = self.findURI(fileURI)
                            if co == None:
                                self.log.console(
                                    'ERROR: Could not find %s in %s'
                                    % (fileURI, repr(self.metadict)),
                                    logging.ERROR)
                            self.observationURI(co, ob)

                # If this plane has inputs, list them here.
                if 'PRVCNT' in header:
                    prvcnt = int(header['PRVCNT'])
                    for i in range(1, prvcnt + 1):
                        prvn = 'PRV%d' % (i)
                        if prvn in header:
                            fileURI = self.fitsfileURI(self.archive,
                                                       header[prvn],
                                                       fits2caom2=False)
                            (co, ob, pl) = self.findURI(fileURI)
                            if co == None:
                                self.log.console(
                                    'ERROR: Could not find %s in %s'
                                    % (fileURI, repr(self.metadict)),
                                    logging.ERROR)
                            self.planeURI(co, ob, pl)

                # The structure of the test FITS files is known, so we can
                # hard-code some details of the test.
                for extension in range(1, header['NUMEXTS'] + 1):
                    artifact_uri = self.fitsextensionURI('TEST',
                                                         header['FILE-ID'],
                                                         [extension])
                    extheader = pyfits.getheader(header['filepath'], extension)
                    self.add_to_fitsuri_dict(artifact_uri,
                                              'extname', extheader['EXTNAME'])
                    self.add_to_fitsuri_dict(artifact_uri,
                                              'field4', extheader['FIELD4'])

                artifact_uri = self.fitsfileURI('TEST', header['FILE-ID'])
                self.add_to_fitsuri_dict(artifact_uri,
                                          'field5', header['FIELD5'])

    def setUp(self):
        """
        Create a set of fits files (file1, ... , file8).
        There are also text files file2cat.txt and filefcat.txt.
        These are organized into two observations (obs1, obs2, obs3, obs4).

        Product A is a raw data type.
        Product B is derived from products of type A.
        Product C is derived from products of type B.
        Product D is a text file derived from a product of type B.  Products
            of type D have file names derived from the type B input by
            appending _cat to the file_id, e.g.
                file.fits -> file_cat.txt.

        The full CAOM2 structure for these files should be
        obs1
            A
                file1
            B
                file2
            D
                file2_cat

        obs2
            A
                file3
            B
                file4
            D
                file4_cat

        obs3
            C
                file5

        obs4
            A
                file6
            B
                file7
            C
                file8
        Note that most tests will create only parts of this full structure.

        The inputs are:
        file2 <- obs1/A = file1
        file2_cat <- obs1/B = file2
        file4 <- obs2/A = file3
        file4_cat <- obs2/B = file4
        file5 <- obs1/B, obs2/B = file2, file4
        file7 <- obs4/A = file6
        file8 <- obs4/B = file7

        The members (only significant for composites) are
        file5 <- obs1, obs2 = file1, file3

        """
        # save the argument vector
        self.argv = sys.argv

        # set up the test envirnonment
        self.testdir = tempfile.mkdtemp()
        # fake data
        fakedata = numpy.arange(10)

        # Create fits files with suitable test headers
        # Files file[1-4].fits will be present in the working directory
        # File file5.fits will be in file5.tar.gz.
        # Files file[6-8].fits will be in file6.ad.
        # There are also some garbage files to be ignored.

        write_fits(os.path.join(self.testdir, 'file1.fits'),
                   numexts=0,
                   obsid='obs1',
                   product='A')

        write_fits(os.path.join(self.testdir, 'file2.fits'),
                   numexts=0,
                   obsid='obs1',
                   product='B',
                   provenance='file1')

        write_fits(os.path.join(self.testdir, 'file3.fits'),
                   numexts=0,
                   obsid='obs2',
                   product='A')

        write_fits(os.path.join(self.testdir, 'file4.fits'),
                   numexts=0,
                   obsid='obs2',
                   product='B',
                   provenance='file3')

        write_fits(os.path.join(self.testdir, 'file5.fits'),
                   numexts=2,
                   obsid='obs3',
                   product='C',
                   member=['file1', 'file3'],
                   provenance=['file2', 'file4'])

        write_fits(os.path.join(self.testdir, 'file6.fits'),
                   numexts=0,
                   obsid='obs4',
                   product='A')

        write_fits(os.path.join(self.testdir, 'file7.fits'),
                   numexts=0,
                   obsid='obs4',
                   product='B',
                   provenance='file6')

        write_fits(os.path.join(self.testdir, 'file8.fits'),
                   numexts=2,
                   obsid='obs4',
                   product='C',
                   provenance='file7')

        # Create a tarball containing the fits files
        tarfilelist = 'file1.fits file2.fits file3.fits file4.fits file5.fits'
        cmd = ('cd ' + self.testdir +
               '; tar cvzf file5.tar.gz ' + tarfilelist +
               '; rm file5.fits')
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise exceptions.RuntimeError('could not create file5.tar.gz: '
                                          + output)

        # Create the cat files
        outstring = commands.getoutput('cat "source list" > ' + self.testdir +
                                       '/file2_cat.txt')
        outstring = commands.getoutput('cat "source list" > ' + self.testdir +
                                       '/file4_cat.txt')

        # Create some random text files that should be filtered out
        outstring = commands.getoutput('cat "some text" > ' + self.testdir +
                                       '/AlineOfText.txt')
        outstring = commands.getoutput('cat "some text" > ' + self.testdir +
                                       '/BlineOfText.txt')

        # Create an instance of TestIngest and add "command line switches"
        self.testingest = testIngest2caom2.TestIngest()
        self.testingest.qsub = False

        self.testingest.archive = 'TEST'
        self.testingest.stream = 'test'
        self.testingest.adput = False

        self.testingest.server = 'DEVSYBASE'
        self.testingest.database = 'dummy'
        self.testingest.schema = 'dbo'

        self.testingest.config = os.path.join(self.testdir,
                                              'test.config')
        self.testingest.default = os.path.join(self.testdir,
                                              'test.default')

        self.testingest.outdir = self.testdir
        self.testingest.test = True
        self.testingest.loglevel = logging.CRITICAL
        self.testingest.console_output = False

        self.testingest.cmpfunc = cmp # sort alphabetically

        self.testingest.dirlist = []
        self.testingest.tarlist = []
        self.testingest.adlist = []
        self.testingest.filelist = []
        self.testingest.sortedlist = []
        # Beware that we get a different log file in a different temporary
        # directory for every test, and that the logs are all deleted in
        # teardown.
        self.testingest.logfile = os.path.join(self.testdir, 'test.log')
        # self.testingest.logfile = 'test.log'
        self.testingest.log = logger(self.testingest.logfile,
                                     self.testingest.loglevel,
                                     self.testingest.console_output)

        # When ingest2caom2 is in test mode, important events are logged to
        # the console, so we can check that they happened by examining the
        # logger's text buffer.  To avoid a buildup of extraneous text,
        # reset the text buffer at the start of each test case.
        self.testingest.log.set_text('')

        # Create an ad file containing file6.fits, file7.fits and file8.fits
        # Save these files in the subdirectory 'save'
        self.savedir = os.path.join(self.testdir, 'save')
        os.mkdir(self.savedir)
        ADFILE = open(os.path.join(self.testdir, 'file6.ad'), 'w')
        for f in ['file6.fits', 'file7.fits', 'file8.fits']:
            file_id = os.path.splitext(f)[0]
            filepath = os.path.join(self.testdir, f)
            self.testingest.filelist.append(filepath)
            self.testingest.sortedlist.append(file_id)
            # if adPut is available, push this file into ad
            if adput_available:
                cmd = 'adPut -a TEST -as test -replace ' + filepath
                status, output = commands.getstatusoutput(cmd)
                if status:
                    self.testingest.log.console(cmd + ': ' + output,
                                                logging.ERROR)
            print >>ADFILE, 'ad:' + self.testingest.archive + '/' + file_id
            os.rename(filepath, os.path.join(self.savedir, f))
        ADFILE.close()

    def tearDown(self):
        """
        Delete the testdir and any files it contains.
        Use walk in case we need to decend into subdirectories in the future.
        """
        for (dirpath, dirnames, filenames) in os.walk(self.testdir,
                                                      topdown=False):
            for filename in filenames:
                os.remove(os.path.join(dirpath, filename))
            for dirname in dirnames:
                os.rmdir(os.path.join(dirpath, dirname))
        os.rmdir(self.testdir)

        # Restore the system argument vector
        sys.argv = self.argv

    def test040_TestIngest_init(self):
        """
        Verify that initializing an instance of ingest2caom2 creates a valid
        empty structure.
        """

        self.assertEqual(self.testingest.uri, '')
        self.assertEqual(self.testingest.memberset, set([]))
        self.assertEqual(self.testingest.inputset, set([]))
        self.assertEqual(self.testingest.containerlist, [])

    def test050_TestIngest_observationURI(self):
        """
        Verify that observationURI returns the expected strings and that
        memberset is updated correctly.
        """
        self.assertEqual(self.testingest.memberset, set([]))

        self.assertEqual(self.testingest.observationURI('A', 'B').uri,
                         'caom:A/B')
        self.assertEqual(self.testingest.observationURI('CCC', 'dddd').uri,
                         'caom:CCC/dddd')
        self.assertEqual(self.testingest.memberset,
                         set([ObservationURI('caom:A/B').uri, 
                              ObservationURI('caom:CCC/dddd').uri]))

        # Verify that setting member=False does not update memberset
        self.assertEqual(self.testingest.observationURI('E', 'F',
                                                        member=False).uri,
                         'caom:E/F')
        self.assertEqual(self.testingest.memberset,
                         set([ObservationURI('caom:A/B').uri, 
                              ObservationURI('caom:CCC/dddd').uri]))

        # verify that adding an existing member does not change memberset
        self.assertEqual(self.testingest.observationURI('A', 'B').uri,
                         'caom:A/B')
        self.assertEqual(self.testingest.memberset,
                         set([ObservationURI('caom:A/B').uri, 
                              ObservationURI('caom:CCC/dddd').uri]))

        self.testingest.clear()
        self.assertEqual(self.testingest.memberset, set([]))

    def test060_TestIngest_planeURI(self):
        """
        Verify that planeURI returns the expected strings and that
        inputset is updated correctly.
        """
        self.assertEqual(self.testingest.inputset, set([]))

        self.assertEqual(self.testingest.planeURI('A', 'B', 'c').uri,
                         'caom:A/B/c')
        self.assertEqual(self.testingest.planeURI('CCC', 'dddd', 'ee').uri,
                         'caom:CCC/dddd/ee')
        self.assertEqual(self.testingest.inputset,
                         set([PlaneURI('caom:A/B/c').uri, 
                              PlaneURI('caom:CCC/dddd/ee').uri]))

        # Verify that setting inoput=False does not update inputset
        self.assertEqual(self.testingest.planeURI('E', 'F', 'g', 
                                                  input=False).uri,
                         'caom:E/F/g')
        self.assertEqual(self.testingest.inputset,
                         set([PlaneURI('caom:A/B/c').uri, 
                              PlaneURI('caom:CCC/dddd/ee').uri]))

        # verify that adding an existing member does not change memberset
        self.assertEqual(self.testingest.planeURI('A', 'B', 'c').uri,
                         'caom:A/B/c')
        self.assertEqual(self.testingest.inputset,
                         set([PlaneURI('caom:A/B/c').uri, 
                              PlaneURI('caom:CCC/dddd/ee').uri]))

        self.testingest.clear()
        self.assertEqual(self.testingest.inputset, set([]))


    def test070_TestIngest_fitsfileURI(self):
        """
        Verify that fitsfileURI returns the expected strings and that
        uri is updated correctly.
        """
        self.assertEqual(self.testingest.uri, '')

        # If fits2caom2 is not specified, it defaults to True
        # so update self.uri
        self.assertEqual(self.testingest.fitsfileURI('A', 'B'),
                         'ad:A/B')
        self.assertEqual(self.testingest.uri, 'ad:A/B')

        self.assertEqual(self.testingest.fitsfileURI('CCC', 'dddd'),
                         'ad:CCC/dddd')
        self.assertEqual(self.testingest.uri, 'ad:CCC/dddd')

        # Verify that setting fits2caom2=False does not update uri
        self.assertEqual(self.testingest.fitsfileURI('E', 'F',
                                                     fits2caom2=False),
                         'ad:E/F')
        self.assertEqual(self.testingest.uri, 'ad:CCC/dddd')

        self.testingest.clear()
        self.assertEqual(self.testingest.uri, '')

    def test080_TestIngest_fitsextensionURI(self):
        """
        Verify that fitsextensionURI returns the expected strings and that
        uri is updated correctly.
        """
        self.ic = testIngest2caom2.TestIngest()
        self.assertEqual(self.testingest.uri, '')

        # If fits2caom2 is not specified, it defaults to True
        # so update self.uri
        self.assertEqual(
            self.testingest.fitsextensionURI('A', 'B', [0]),
            'ad:A/B#[0]')
        self.assertEqual(self.testingest.uri, 'ad:A/B')

        self.assertEqual(
            self.testingest.fitsextensionURI('CCC', 'dddd', [0, 1], 
                                             fits2caom2=False),
            'ad:CCC/dddd#[0,1]')
        self.assertEqual(self.testingest.uri, 'ad:A/B')

        self.assertEqual(
            self.testingest.fitsextensionURI('CCC', 'dddd', [(0, 1)], 
                                             fits2caom2=False),
            'ad:CCC/dddd#[0-1]')
        self.assertEqual(
            self.testingest.fitsextensionURI('CCC', 'dddd', [(0, 1), (3, 4), 6], 
                                             fits2caom2=False),
            'ad:CCC/dddd#[0-1,3-4,6]')
        
        self.assertRaises(logger.LoggerError,
                          self.testingest.fitsextensionURI,
                          'A', 'b', [(1,2,3)])
        self.assertRaises(logger.LoggerError,
                          self.testingest.fitsextensionURI,
                          'A', 'b', [[1,2]])
        self.assertRaises(logger.LoggerError,
                          self.testingest.fitsextensionURI,
                          'A', 'b', [1.0, 2.0])
                          
        self.testingest.clear()
        self.assertEqual(self.testingest.uri, '')

    def test100_TestIngest_add_to_plane_dict(self):
        """
        Verify that add_to_plane_dict adds key-value pairs to the plane_dict.
        """
        self.assertEqual(self.testingest.plane_dict, {})

        self.testingest.add_to_plane_dict('a', 'b')
        self.assertEqual(self.testingest.plane_dict, {'a': 'b'})
        self.testingest.add_to_plane_dict('c', 'd')
        self.assertEqual(self.testingest.plane_dict, {'a': 'b',
                                                            'c': 'd'})

        # verify that using an already existing ket overwrites the value
        self.testingest.add_to_plane_dict('c', 'e')
        self.assertEqual(self.testingest.plane_dict, {'a': 'b',
                                                            'c': 'e'})

        # If an attempt is made to add a non-string value, a logger.LoggerError
        # is raised
        self.assertRaises(logger.LoggerError,
                          self.testingest.add_to_plane_dict,
                          'c',
                          1)

        self.testingest.clear()
        self.assertEqual(self.testingest.plane_dict, {})

    def test120_TestIngest_add_to_fitsuri_dict(self):
        """
        Verify that add_to_plane_dict adds key-value pairs to the plane_dict.
        """
        uri = self.testingest.fitsfileURI('TEST', 'test')
        self.assertTrue('custom' in self.testingest.fitsuri_dict[uri])

        self.testingest.add_to_fitsuri_dict(uri, 'a', 'b')
        self.assertTrue('a' in self.testingest.fitsuri_dict[uri]
                        and self.testingest.fitsuri_dict[uri]['a'] == 'b',
                        'fitsuri_dict[uri]["a"] != "b"')
        self.testingest.add_to_fitsuri_dict(uri, 'c', 'd')
        self.assertEqual(len(self.testingest.fitsuri_dict[uri]), 3)
        self.assertTrue('c' in self.testingest.fitsuri_dict[uri]
                        and self.testingest.fitsuri_dict[uri]['c'] == 'd',
                        'fitsuri_dict[uri]["c"] != "d"')

        # verify that using an already existing ket overwrites the value
        self.testingest.add_to_fitsuri_dict(uri, 'c', 'e')
        self.assertEqual(len(self.testingest.fitsuri_dict[uri]), 3)
        self.assertTrue('a' in self.testingest.fitsuri_dict[uri]
                        and self.testingest.fitsuri_dict[uri]['a'] == 'b',
                        'fitsuri_dict[uri]["a"] != "b"')
        self.assertTrue('c' in self.testingest.fitsuri_dict[uri]
                        and self.testingest.fitsuri_dict[uri]['c'] == 'e',
                        'fitsuri_dict[uri]["c"] != "e"')

        # If an attempt is made to add a non-string value, a logger.LoggerError
        # is raised
        self.assertRaises(logger.LoggerError,
                      self.testingest.add_to_fitsuri_dict, uri, 'c', 1)

        self.testingest.clear()
        self.assertEqual(self.testingest.fitsuri_dict, {})

    def test130_TestIngest_findURI(self):
        """
        Verify that findURI can find the collection, observation and plane
        for an fitsfileURI.
        """
        uri_dict1 = \
            OrderedDict([(self.testingest.fitsfileURI('TEST', 'file1',
                                              fits2caom2=False), None),
                         (self.testingest.fitsfileURI('TEST', 'file2',
                                              fits2caom2=False), None)])
        uri_dict2 = \
            OrderedDict([(self.testingest.fitsfileURI('TEST', 'file3',
                                              fits2caom2=False), None),
                        (self.testingest.fitsfileURI('TEST', 'file4',
                                              fits2caom2=False), None)])
        uri_dict3 = \
            OrderedDict([(self.testingest.fitsfileURI('TEST', 'file5',
                                              fits2caom2=False), None),
                         (self.testingest.fitsfileURI('TEST', 'file6',
                                              fits2caom2=False), None)])

        self.testingest.metadict = \
            {'TEST': {'obs1': {'a': {'uri_dict': uri_dict1},
                               'b': {'uri_dict': uri_dict2}},
                      'obs2': {'c': {'uri_dict': uri_dict3}}}}

        testvalues = {'file1': ('TEST', 'obs1', 'a'),
                      'file2': ('TEST', 'obs1', 'a'),
                      'file3': ('TEST', 'obs1', 'b'),
                      'file4': ('TEST', 'obs1', 'b'),
                      'file5': ('TEST', 'obs2', 'c'),
                      'file6': ('TEST', 'obs2', 'c')}

        #Verify that files in uri_dict can be found
        for file_id in testvalues:
            fileURI = \
                self.testingest.fitsfileURI('TEST', file_id, fits2caom2=False)
            self.assertEqual(self.testingest.findURI(fileURI),
                             testvalues[file_id])

        #Verify that bogus files return a tuple of Nones
        fileURI = self.testingest.fitsfileURI('TEST', 'bogus',
                                              fits2caom2=False)
        self.assertEqual(self.testingest.findURI(fileURI),
                         (None, None, None))

    def test140_TestIngest_get_plane_value(self):
        """
        Verify that we get back values in the plane_dict
        """
        pldict1 = {'x': 'X1', 'y': 'Y1', 'z': 'Z1'}
        pldict2 = {'x': 'X2', 'y': 'Y2', 'z': 'Z2'}
        pldict3 = {'x': 'X3', 'y': 'Y3', 'z': 'Z3'}

        self.testingest.metadict = \
            {'TEST': {'obs1': {'a': {'plane_dict': pldict1},
                               'b': {'plane_dict': pldict2}},
                      'obs2': {'c': {'plane_dict': pldict3}}}}

        dictdict = {'obs1': {'a': pldict1,
                             'b': pldict2},
                    'obs2': {'c': pldict3}}

        for obs in dictdict:
            for pl in dictdict[obs]:
                for key in dictdict[obs][pl]:
                    self.assertEqual(self.testingest.get_plane_value('TEST',
                                                                     obs,
                                                                     pl,
                                                                     key),
                                     dictdict[obs][pl][key])

        # verify that we get KeyError for bat index values
        self.assertRaises(KeyError,
            self.testingest.get_plane_value, 'BOGUS', 'obs1', 'a', 'x')
        self.assertRaises(KeyError,
            self.testingest.get_plane_value, 'TEST', 'bogus', 'a', 'x')
        self.assertRaises(KeyError,
            self.testingest.get_plane_value, 'TEST', 'obs1', 'bogus', 'x')
        self.assertRaises(KeyError,
            self.testingest.get_plane_value, 'TEST', 'obs1', 'a', 'bogus')

    def test150_TestIngest_get_artifact_value(self):
        """
        Verify that we get back values in the plane_dict
        """
        ardict1 = {'uri1': {'x': 'X1', 'y': 'Y1', 'z': 'Z1'},
                   'uri2': {'x': 'X2', 'y': 'Y2', 'z': 'Z2'}}
        ardict2 = {'uri3': {'x': 'X3', 'y': 'Y3', 'z': 'Z3'},
                   'uri4': {'x': 'X4', 'y': 'Y4', 'z': 'Z4'}}
        ardict3 = {'uri5': {'x': 'X5', 'y': 'Y5', 'z': 'Z5'},
                   'uri6': {'x': 'X6', 'y': 'Y6', 'z': 'Z6'}}

        self.testingest.metadict = \
            {'TEST': {'obs1': {'a': {'fitsuri_dict': ardict1},
                               'b': {'fitsuri_dict': ardict2}},
                      'obs2': {'c': {'fitsuri_dict': ardict3}}}}

        dictdict = {'obs1': {'a': ardict1,
                             'b': ardict2},
                    'obs2': {'c': ardict3}}

        for obs in dictdict:
            for pl in dictdict[obs]:
                for uri in dictdict[obs][pl]:
                    for key in dictdict[obs][pl][uri]:
                        self.assertEqual(
                            self.testingest.get_artifact_value('TEST',
                                                               obs,
                                                               pl,
                                                               uri,
                                                               key),
                                     dictdict[obs][pl][uri][key])

        # verify that we get KeyError for bat index values
        self.assertRaises(KeyError,
            self.testingest.get_artifact_value,
            'BOGUS', 'obs1', 'a', 'uri1', 'x')
        self.assertRaises(KeyError,
            self.testingest.get_artifact_value,
            'TEST', 'bogus', 'a', 'uri1', 'x')
        self.assertRaises(KeyError,
            self.testingest.get_artifact_value,
            'TEST', 'obs1', 'bogus', 'uri1', 'x')
        self.assertRaises(KeyError,
            self.testingest.get_artifact_value,
            'TEST', 'obs1', 'a', 'bogus', 'x')
        self.assertRaises(KeyError,
            self.testingest.get_artifact_value,
            'TEST', 'obs1', 'a', 'uri1', 'bogus')

    def test160_TestIngest_commandLineSwitches(self):
        """
        Verify that command line switches are parse correctly
        """
        # Verify that the defaults set in __init__ are still in place
        self.assertEqual(self.testingest.qsub, False)
        self.assertEqual(self.testingest.archive, 'TEST')
        self.assertEqual(self.testingest.stream, 'test')
        self.assertEqual(self.testingest.adput, False)
        self.assertEqual(self.testingest.database, 'dummy')
        self.assertEqual(self.testingest.schema, 'dbo')
        self.assertEqual(self.testingest.config, os.path.join(self.testdir,
                                              'test.config'))
        self.assertEqual(self.testingest.default, os.path.join(self.testdir,
                                              'test.default'))
        self.assertEqual(self.testingest.outdir, self.testdir)
        self.assertEqual(self.testingest.test, True)
        self.assertEqual(self.testingest.loglevel, logging.CRITICAL)

        # reset some of the flags
        self.testingest.test = True

        # Set switches to verify that they are interpreted correctly
        sys.argv = ['TestIngest.py',
                    '--qsub',
                    '--archive=MYTEST',
                    '--stream=MYSTREAM',
                    '--adput',
                    '--database=MYDATABASE',
                    '--schema=MYSCHEMA',
                    '--config=MYCONFIG',
                    '--default=MYDEFAULT',
                    '--outdir=' + os.path.join(self.testdir, 'MYOUTDIR'),
                    '--test',
                    '--log=MYLOG',
                    '--quiet',
                    os.path.join(self.testdir, 'file1.fits'),
                    os.path.join(self.testdir, 'file2.fits'),
                    os.path.join(self.testdir, 'file5.tar.gz'),
                    os.path.abspath('.')]
        self.testingest.defineCommandLineSwitches()
        self.testingest.processCommandLineSwitches()

        # Verify that the switches are set as requested
        self.assertEqual(self.testingest.qsub, True)
        self.assertEqual(self.testingest.archive, 'MYTEST')
        self.assertEqual(self.testingest.stream, 'MYSTREAM')
        self.assertEqual(self.testingest.adput, True)
        self.assertEqual(self.testingest.database, 'MYDATABASE')
        self.assertEqual(self.testingest.schema, 'MYSCHEMA')
        self.assertEqual(self.testingest.config,
                         os.path.abspath('MYCONFIG'))
        self.assertEqual(self.testingest.default,
                         os.path.abspath('MYDEFAULT'))
        self.assertEqual(self.testingest.outdir,
                         os.path.join(self.testdir, 'MYOUTDIR'))
        self.assertEqual(self.testingest.test, True)
        self.assertEqual(self.testingest.loglevel, logging.WARN)

        # Some particular tests
        sys.argv = ['TestIngest.py',
                    '--test',
                    '--verbose',
                    os.path.join(self.testdir, 'file1.fits')]
        self.testingest.defineCommandLineSwitches()
        self.testingest.processCommandLineSwitches()
        self.assertEqual(self.testingest.loglevel, logging.DEBUG)

        # Restore quiet logging
        sys.argv = ['TestIngest.py',
                    '--test',
                    '--quiet',
                    os.path.join(self.testdir, 'file1.fits')]
        self.testingest.defineCommandLineSwitches()
        self.testingest.processCommandLineSwitches()
        self.assertEqual(self.testingest.loglevel, logging.WARN)

    @unittest.skipIf(not adput_available, 'adPut is not available on this system')
    def test180_TestIngest_verifyFileInAD(self):
        """
        Try to push the file into ad and then check that it is in ad, if
        adPut is available on this system
        """
        self.testingest.verifyFileInAD(
            os.path.join(self.testdir, 'file1.fits'))
        self.testingest.verifyFileInAD(
            os.path.join(self.testdir, 'file2.fits'))

    def test190_TestIngest_submitJobToGridEngine(self):
        """
        Verify that submitting a container creates the cshfile and a
        file containing the pickled container.
        """
        for container in self.testingest.containerlist:
            (csh, pickle) = self.testingest.submitJobToGridEngine(container)
            self.assertTrue(os.path.exists(csh))
            self.assertTrue(os.path.exists(pickle))
            status, output = commands.getstatusoutput(
                'grep ' + pickle + ' ' + csh)
            # The line containing the pickle file is the command line
            # look for idividual switches
            self.assertTrue(
                re.search(r'^' + re.escape(os.path.abspath(sys.argv[0])),
                          output))
            self.assertFalse(re.search(r'--qsub', output))
            self.assertTrue(
                re.search(r'--archive=' + self.testingest.archive, output))
            self.assertTrue(
                re.search(r'--stream=' + self.testingest.stream, output))
            self.assertTrue(re.search(r'--adput', output))
            self.assertTrue(
                re.search(r'--server=' + self.testingest.server, output))
            self.assertTrue(
                re.search(r'--database=' + self.testingest.database, output))
            self.assertTrue(
                re.search(r'--schema=' + self.testingest.schema, output))
            self.assertTrue(
                re.search(r'--config=' +
                          re.escape(self.testingest.config),
                          output))
            self.assertTrue(
                re.search(r'--default=' +
                          re.escape(self.testingest.default),
                          output))
            self.assertTrue(
                re.search(r'--outdir=' +
                          re.escape(self.testingest.outdir),
                          output))
            self.assertTrue(
                re.search(r'--logfile=' +
                          re.escape(self.testingest.logfile),
                          output))
            self.assertTrue(re.search(r'--test', output))

            # Now verify that the pickled container can be restored
            with open(pickle, 'r') as PKL:
                self.assertEqual(cpickle.load(PKL), container)


    def test200_TestIngest_commandLineContainers(self):
        """
        Verify that submitting all of the containers creates the expected
        number of csh files.  Note we have already verified the
        contents.
        
        This command line contains three containers:
            tarfile_container = file5.tar.gz
            filelist_container = ./
            filelistcontainer = ['file1.fits', 'file2.fits']
        """
        sys.argv = ['TestIngest.py',
                    '--qsub',
                    '--archive=MYTEST',
                    '--stream=MYSTREAM',
                    '--adput',
                    '--database=MYDATABASE',
                    '--schema=MYSCHEMA',
                    '--config=MYCONFIG',
                    '--default=MYDEFAULT',
                    '--outdir=' + os.path.join(self.testdir, 'MYOUTDIR'),
                    '--test',
                    '--log=MYLOG.log',
                    '--quiet',
                    os.path.join(self.testdir, 'file1.fits'),
                    os.path.join(self.testdir, 'file2.fits'),
                    os.path.join(self.testdir, 'file5.tar.gz')]
        self.testingest.defineCommandLineSwitches()
        self.testingest.processCommandLineSwitches()
        self.testingest.commandLineContainers()
        dir = os.path.dirname(self.testingest.logfile)
        numcsh = 0
        for filename in os.listdir(dir):
            base, ext = os.path.splitext(filename)
            if ext == '.csh':
                numcsh += 1
        self.assertEqual(numcsh, 2)

    def test210_testIngest_build_dict(self):
        """
        Verify that the example provided by build_dict actually produces the
        expected dictionaries.  Note that file1.fits has only
        the primary HDU (see setup), no inputs and no members.
        """

        # Note that we have to build self.testingest.metadict before the
        # member and input functions can work.  Run this test only on files
        # with no inputs or members.

        filepath = os.path.join(self.testdir, 'file1.fits')
        primary_header = pyfits.getheader(filepath)
        primary_header.update('filepath', filepath)
        primary_header.update('file_id', primary_header['FILE-ID'])

        self.testingest.build_dict(primary_header)
        self.assertEqual(self.testingest.memberset, set([]))
        self.assertEqual(self.testingest.inputset, set([]))

        self.assertEqual(len(self.testingest.plane_dict), 3)
        self.assertEqual(self.testingest.collection, 'TEST')
        self.assertEqual(self.testingest.observationID, 'obs1')
        self.assertEqual(self.testingest.productID, 'A')
        self.assertEqual(self.testingest.plane_dict['field1'], 'F1A')
        self.assertEqual(self.testingest.plane_dict['field2'], 'F2A')
        self.assertEqual(self.testingest.plane_dict['field3'], 'NOVALUE')

        self.assertEqual(self.testingest.uri, 'ad:TEST/file1')

        self.assertEqual(self.testingest.fitsuri_dict.keys(),
                         ['ad:TEST/file1'])
        self.assertEqual(
            len(self.testingest.fitsuri_dict['ad:TEST/file1']), 2)
        self.assertEqual(
            self.testingest.fitsuri_dict['ad:TEST/file1']['field5'], 'GOOD')

    def test215_testIngest_build_metadict(self):
        """
        Verify that build_dict correctly fills matadict from the metadata supplied
        by the internal structures.
        """
        self.testingest.collection = 'TEST'
        self.testingest.observationID = 'obs1'
        self.testingest.productID = 'A'
        self.testingest.plane_dict = {'field': 'value'}

        self.testingest.uri = 'ad:TEST/file1'
        self.testingest.memberset = set(['a', 'b'])
        self.testingest.inputset = set(['c', 'd'])

        self.testingest.fitsuri_dict = {
            'ad:TEST/file1#[0]': {'Q': 'valueQ'},
            'ad:TEST/file1': {'R': 'valueR'}}
        self.testingest.override_items = 9

        self.testingest.build_metadict('file1.fits', True)

        md = self.testingest.metadict
        self.assertEqual(set(md.keys()), set(['TEST']))
        self.assertEqual(set(md['TEST']['obs1'].keys()),
                         set(['memberset', 'A']))
        self.assertEqual(md['TEST']['obs1']['memberset'], set(['a', 'b']))

        pl = md['TEST']['obs1']['A']
        self.assertEqual(len(pl), 5)
        self.assertEqual(set(pl.keys()),
                         set(['plane_dict',
                              'inputset',
                              'uri_dict',
                              'ad:TEST/file1#[0]',
                              'ad:TEST/file1']))
        self.assertEqual(pl['inputset'], set(['c', 'd']))
        self.assertEqual(pl['uri_dict'], OrderedDict([('ad:TEST/file1', 
                                                       'file1.fits')]))

        self.assertEqual(len(pl['plane_dict']), 1)
        self.assertEqual(pl['plane_dict']['field'], 'value')

        fd = pl['ad:TEST/file1']
        self.assertEqual(fd['R'], 'valueR')

    def test220_testIngest_fillMetadictFromFile(self):
        """
        Verify that the example provided by TestIngest actually produces the
        expected dictionaries.  Note that file1.fits has only
        the primary HDU (see setup), no inputs and no members.
        """

        # Note that we have to build self.testingest.metadict before the
        # member and input functions can work.  Run this test only on files
        # with no inputs or members.

        filepath = os.path.join(self.testdir, 'file1.fits')
        self.testingest.fillMetadictFromFile('file1', filepath, False)
        md = self.testingest.metadict
        self.assertEqual(set(md.keys()), set(['TEST']))
        self.assertEqual(set(md['TEST']['obs1'].keys()),
                         set(['memberset', 'A']))
        self.assertEqual(md['TEST']['obs1']['memberset'], set([]))

        pl = md['TEST']['obs1']['A']
        self.assertEqual(len(pl), 4)
        self.assertEqual(set(pl.keys()),
                         set(['plane_dict',
                              'inputset',
                              'uri_dict',
                              'ad:TEST/file1']))
        self.assertEqual(pl['inputset'], set([]))
        self.assertEqual(pl['uri_dict'], OrderedDict([('ad:TEST/file1', None)]))

        self.assertEqual(len(pl['plane_dict']), 3)
        self.assertEqual(pl['plane_dict']['field1'], 'F1A')
        self.assertEqual(pl['plane_dict']['field2'], 'F2A')
        self.assertEqual(pl['plane_dict']['field3'], 'NOVALUE')

        fd = pl['ad:TEST/file1']
        self.assertEqual(fd['field5'], 'GOOD')

    def test230_testIngest_fillMetadict_filelist(self):
        """
        Verify that the example provided by TestIngest actually produces the
        expected dictionaries for the filelist_container.
        """
        ###########################################
        # Set command line for file list container
        ###########################################
        sys.argv = ['TestIngest.py',
                    '--quiet',
                    os.path.join(self.testdir, 'file1.fits'),
                    os.path.join(self.testdir, 'file2.fits'),
                    os.path.join(self.testdir, 'file2_cat.txt'),
                    os.path.join(self.testdir, 'file3.fits'),
                    os.path.join(self.testdir, 'file4.fits'),
                    os.path.join(self.testdir, 'file4_cat.txt')]

        # For this test there should be no filtering or sorting of files
        self.testingest.filterfunc = nofilter

        self.testingest.defineCommandLineSwitches()
        self.testingest.processCommandLineSwitches()
        self.testingest.commandLineContainers()

        # Verify that we have one filelist_container
        self.assertEqual(len(self.testingest.containerlist), 1)
        self.assertTrue(isinstance(self.testingest.containerlist[0],
                                   filelist_container))
        self.assertEqual(
            set(self.testingest.containerlist[0].filedict.keys()),
            set(['file1', 'file2', 'file2_cat',
                 'file3', 'file4', 'file4_cat']))

        self.testingest.fillMetadict(self.testingest.containerlist[0])

        # Inspect some of the fields in metadict
        md = self.testingest.metadict
        self.assertEqual(set(md.keys()), set(['TEST']))
        self.assertEqual(set(md['TEST'].keys()),
                         set(['obs1', 'obs2']))
        self.assertEqual(set(md['TEST']['obs1'].keys()),
                         set(['memberset', 'A', 'B', 'D']))
        self.assertEqual(md['TEST']['obs1']['memberset'], set([]))

        self.assertEqual(set(md['TEST']['obs2'].keys()),
                         set(['memberset', 'A', 'B', 'D']))
        self.assertEqual(md['TEST']['obs2']['memberset'], set([]))

        pl = md['TEST']['obs1']['B']
        self.assertEqual(set(pl.keys()),
                         set(['plane_dict',
                              'inputset',
                              'uri_dict',
                              'ad:TEST/file2']))
        self.assertEqual(pl['inputset'], 
                         set([PlaneURI('caom:TEST/obs1/A').uri]))
        
        file2path = os.path.join(self.testdir, 'file2.fits')
        self.assertEqual(pl['uri_dict'], 
                         OrderedDict([('ad:TEST/file2', file2path)]))

        self.assertEqual(len(pl['plane_dict']), 3)
        self.assertEqual(pl['plane_dict']['field1'], 'F1B')
        self.assertEqual(pl['plane_dict']['field2'], 'F2B')
        self.assertEqual(pl['plane_dict']['field3'], 'F3B')

        fd = pl['ad:TEST/file2']
        self.assertEqual(fd['field5'], 'GOOD')

        pl = md['TEST']['obs1']['D']
        self.assertEqual(set(pl.keys()),
                         set(['plane_dict',
                              'inputset',
                              'uri_dict',
                              'ad:TEST/file2_cat']))
        self.assertEqual(pl['inputset'], 
                         set([PlaneURI('caom:TEST/obs1/B').uri]))
        file2catpath = os.path.join(self.testdir, 'file2_cat.txt')
        self.assertEqual(pl['uri_dict'], 
                         OrderedDict([('ad:TEST/file2_cat', file2catpath)]))

    def test240_testIngest_fillMetadict_tarfile(self):
        """
        Verify that the example provided by TestIngest actually produces the
        expected dictionaries for the tarfile_container.
        """
        ###########################################
        # Set command line for tar file container
        ###########################################
        sys.argv = ['TestIngest.py',
                    '--quiet',
                    os.path.join(self.testdir, 'file5.tar.gz')]
        self.testingest.metadict = {}
        self.testingest.defineCommandLineSwitches()
        self.testingest.processCommandLineSwitches()
        self.testingest.commandLineContainers()

        # Verify that we have one tarfile_container
        self.assertEqual(len(self.testingest.containerlist), 1)
        self.assertTrue(isinstance(self.testingest.containerlist[0],
                                   tarfile_container))
        self.assertEqual(
            set(self.testingest.containerlist[0].filedict.keys()),
            set(['file1', 'file2', 'file3', 'file4', 'file5']))

        self.testingest.fillMetadict(self.testingest.containerlist[0])

        # Inspect some of the fields in metadict
        md = self.testingest.metadict
        self.assertEqual(set(md.keys()), set(['TEST']))
        self.assertEqual(set(md['TEST'].keys()),
                         set(['obs1', 'obs2', 'obs3']))
        self.assertEqual(set(md['TEST']['obs3'].keys()),
                         set(['memberset', 'C']))
        self.assertEqual(md['TEST']['obs3']['memberset'],
                         set([ObservationURI('caom:TEST/obs1').uri, 
                              ObservationURI('caom:TEST/obs2').uri]))

        pl = md['TEST']['obs3']['C']
        self.assertEqual(set(pl.keys()),
                         set(['plane_dict',
                              'inputset',
                              'uri_dict',
                              'ad:TEST/file5#[1]',
                              'ad:TEST/file5#[2]',
                              'ad:TEST/file5']))
        self.assertEqual(pl['inputset'],
                         set([PlaneURI('caom:TEST/obs1/B').uri, 
                              PlaneURI('caom:TEST/obs2/B').uri]))
        self.assertEqual(pl['uri_dict'], OrderedDict([('ad:TEST/file5', None)]))

        self.assertEqual(len(pl['plane_dict']), 3)
        self.assertEqual(pl['plane_dict']['field1'], 'F1C')
        self.assertEqual(pl['plane_dict']['field2'], 'F2C')
        self.assertEqual(pl['plane_dict']['field3'], 'F3C')

        fd = pl['ad:TEST/file5']
        self.assertEqual(fd['field5'], 'GOOD')

    @unittest.skipIf(not adput_available, 'adPut is not available on this system')
    def test250_testIngest_fillMetadict_adfile(self):
        """
        Verify that the example provided by TestIngest actually produces the
        expected dictionaries for the adfile_container.
        """
        ###########################################
        # Set command line for ad file container
        ###########################################
        sys.argv = ['TestIngest.py',
                    '--quiet',
                    'ad:' + os.path.join(self.testdir, 'file6.ad')]
        self.testingest.metadict = {}
        self.testingest.defineCommandLineSwitches()
        self.testingest.processCommandLineSwitches()
        self.testingest.commandLineContainers()

        # Verify that we have one filelist_container
        self.assertEqual(len(self.testingest.containerlist), 1)
        self.assertTrue(isinstance(self.testingest.containerlist[0],
                                   adfile_container))
        self.assertEqual(
            set(self.testingest.containerlist[0].filedict.keys()),
            set(['file6', 'file7', 'file8']))

        self.testingest.fillMetadict(self.testingest.containerlist[0])

        # Inspect some of the fields in metadict
        md = self.testingest.metadict
        self.assertEqual(set(md.keys()),
                         set(['TEST']))
        self.assertEqual(set(md['TEST'].keys()),
                         set(['obs4']))
        self.assertEqual(set(md['TEST']['obs4'].keys()),
                         set(['memberset', 'A', 'B', 'C']))
        self.assertEqual(md['TEST']['obs4']['memberset'], set([]))

        myuri = {'A': 'ad:TEST/file6',
                 'B': 'ad:TEST/file7',
                 'C': 'ad:TEST/file8'}
        for plane in md['TEST']['obs4']:
            if plane not in ['memberset']:
                pl = md['TEST']['obs4'][plane]
                if plane == 'C':
                    self.assertEqual(set(pl.keys()),
                                     set(['plane_dict',
                                          'inputset',
                                          'uri_dict',
                                          myuri[plane],
                                          myuri[plane] + '#[1]',
                                          myuri[plane] + '#[2]']))
                else:
                    self.assertEqual(set(pl.keys()),
                                     set(['plane_dict',
                                          'inputset',
                                          'uri_dict',
                                          myuri[plane]]))

                if plane == 'B':
                    self.assertEqual(pl['inputset'], set(['caom:TEST/obs4/A']))
                elif plane == 'C':
                    self.assertEqual(pl['inputset'], set(['caom:TEST/obs4/B']))

                self.assertEqual(pl['uri_dict'], OrderedDict([(myuri[plane], 
                                                               None)]))

                self.assertEqual(len(pl['plane_dict']), 3)
                self.assertEqual(pl['plane_dict']['field1'], 'F1' + plane)
                self.assertEqual(pl['plane_dict']['field2'], 'F2' + plane)
                if plane == 'A':
                    self.assertEqual(pl['plane_dict']['field3'], 'NOVALUE')
                else:
                    self.assertEqual(pl['plane_dict']['field3'], 'F3' + plane)

                fd = pl[myuri[plane]]
                self.assertEqual(fd['field5'], 'GOOD')

    def test260_testIngest_fillMetadict(self):
        """
        Verify that the example provided by TestIngest does not crash
        with multiple containers.
        """
        file_list = [os.path.join(self.testdir, 'file1.fits'),
                     os.path.join(self.testdir, 'file2.fits'),
                     os.path.join(self.testdir, 'file3.fits'),
                     os.path.join(self.testdir, 'file4.fits'),
                     os.path.join(self.testdir, 'file5.tar.gz')]
        if adput_available:
            file_list.append('ad:' + os.path.join(self.testdir, 'file6.ad'))

        sys.argv = ['TestIngest.py',
                    '--quiet'] + file_list
        self.testingest.metadict = {}
        self.testingest.defineCommandLineSwitches()
        self.testingest.processCommandLineSwitches()
        self.testingest.commandLineContainers()
        self.testingest.fillMetadict(self.testingest.containerlist[0])

    def test270_testIngest_writeOverrideFile(self):
        """
        Verify that the override file contains sections for
        - the plane
        - each fitsuri in the correct order
        """
        self.testingest.metadict['TEST'] = OrderedDict()
        thisCollection = self.testingest.metadict['TEST']
        
        thisCollection['obs3'] = OrderedDict()
        thisObservation = thisCollection['obs3']
        thisObservation['memberset'] = set([])
        
        thisObservation['C'] = OrderedDict()
        thisPlane = thisObservation['C']
        thisPlane['inputset'] = set([])
        thisPlane['uri_dict'] = OrderedDict([('ad:TEST/file5', None)])
        
        thisPlane['plane_dict'] = OrderedDict()
        thisPlane['plane_dict']['key1'] = 'value1' 
        
        thisPlane['ad:TEST/file5#[1]'] = OrderedDict()
        thisFitsuri = thisPlane['ad:TEST/file5#[1]']
        thisFitsuri['key2'] = 'value2'
        thisFitsuri['key3'] = 'value3'
        
        thisPlane['ad:TEST/file5#[2]'] = OrderedDict()
        thisFitsuri = thisPlane['ad:TEST/file5#[2]']
        thisFitsuri['key3'] = 'value3'
        thisFitsuri['key4'] = 'value4'
        
        thisPlane['ad:TEST/file5'] = OrderedDict()
        thisFitsuri = thisPlane['ad:TEST/file5']
        thisFitsuri['key2'] = 'value2'
        thisFitsuri['key4'] = 'value4'

        filepath = self.testingest.writeOverrideFile('TEST', 'obs3', 'C')

        # read the override file and strip out any blank lines
        override = []
        with open(filepath) as OVER:
            override = OVER.readlines()
        override = [l.strip() for l in override]
        override = [l for l in override if l]

        self.assertEqual(override,
            ['key1                           = value1',
             '?ad:TEST/file5#[1]',
             'key2                           = value2',
             'key3                           = value3',
             '?ad:TEST/file5#[2]',
             'key3                           = value3',
             'key4                           = value4',
             '?ad:TEST/file5',
             'key2                           = value2',
             'key4                           = value4'])

    def test340_ignore_artifact(self):
        """
        Verify that calling ignore_artifact empties the plane_dict and
        that subsequent calls to build_metadict skip processing
        """
        filepath = os.path.join(self.testdir, 'file1.fits')
        primary_header = pyfits.getheader(filepath)
        primary_header.update('filepath', filepath)
        primary_header.update('file_id', primary_header['FILE-ID'])

        self.testingest.build_dict(primary_header)
        self.testingest.ignore_artifact('test message')

        self.assertEqual(self.testingest.plane_dict, {})
        self.testingest.build_metadict('file1.fits', False)
        self.assertEqual(self.testingest.metadict, {})

    def test350_reject_plane(self):
        """
        Verify that calling ignore_artifact empties the plane_dict and
        that subsequent calls to build_metadict skip processing.
        
        Beware that this does not actually verify that the plane is not
        ingested, since that would require processing an actual observation,
        and no suitable observation exists in the CAOM-2 repository.  The
        test can only verify that the reject flag propagates to the point that
        ingestfromMetadict could see it.
        """
        filepath = os.path.join(self.testdir, 'file1.fits')
        primary_header = pyfits.getheader(filepath)
        primary_header.update('filepath', filepath)
        primary_header.update('file_id', primary_header['FILE-ID'])

        self.testingest.build_dict(primary_header)

        self.testingest.reject_plane('test message')

        self.assertTrue(ingest2caom2.REJECT in self.testingest.plane_dict)

        self.testingest.build_metadict(filepath, False)
        md = self.testingest.metadict

        self.assertEqual(set(md.keys()), set(['TEST']))
        self.assertEqual(set(md['TEST']['obs1'].keys()),
                         set(['memberset', 'A']))
        self.assertEqual(md['TEST']['obs1']['memberset'], set([]))

        pl = md['TEST']['obs1']['A']
        self.assertEqual(len(pl), 4)
        self.assertEqual(set(pl.keys()),
                         set(['plane_dict',
                              'inputset',
                              'uri_dict',
                              'ad:TEST/file1']))
        self.assertEqual(pl['inputset'], set([]))
        self.assertEqual(pl['uri_dict'], OrderedDict([('ad:TEST/file1', None)]))

        self.assertTrue(ingest2caom2.REJECT in pl['plane_dict'])


if __name__ == '__main__':
    unittest.main()

