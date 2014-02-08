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
#*   Script Name:    testTarfileContainer.py
#*
#*   Purpose:
#+    Unit test module for tools4caom2.tarfile_container
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


import commands
from datetime import datetime
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

from tools4caom2.ingest2caom2 import make_file_id
from tools4caom2.ingest2caom2 import fitsfilter
from tools4caom2.basecontainer import basecontainer
from tools4caom2.tarfile_container import tarfile_container

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

class testTarfileContainer(unittest.TestCase):
    """
    unit tests for tools4caom2.TarfileContainer classe
    """
    def setUp(self):
        """
        Create a set of fits files (file1, ... , file5) and store them in
        a tarfile.
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

        # Add a non-FITS file to verify filtering
        TEXT = open(os.path.join(self.testdir, 'file9.txt'), 'w')
        print >>TEXT, "This is some text"
        TEXT.close()

        # Create a tarball containing the fits files
        tarfilelist = ['file1.fits',
                       'file2.fits',
                       'file3.fits',
                       'file4.fits',
                       'file5.fits',
                       'file9.txt']
        tarfilestring = ' '.join(tarfilelist)
                                 
        cmd = ('cd ' + self.testdir +
               '; tar cvzf file5.tar.gz ' + tarfilestring +
               '; rm ' + tarfilestring)
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise exceptions.RuntimeError('could not create file5.tar.gz: '
                                          + output)

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

    # Test tarfile_container implementations
    def test010_tarfile_container_no_filtering(self):
        test_file_id_list = ['file1', 
                             'file2', 
                             'file3', 
                             'file4', 
                             'file5', 
                             'file9']
        test_file_list = [os.path.join(self.testdir, f) for f in
                          ['file1.fits', 
                           'file2.fits', 
                           'file3.fits',
                           'file4.fits',
                           'file5.fits',
                           'file9.txt']]
        # The gzipped tar file exists so creation of a tarfile_container
        # should succeed.
        # The filterfunc is None.
        tarfilepath = os.path.join(self.testdir, 'file5.tar.gz')
        fc = tarfile_container(tarfilepath, 
                               self.testdir, 
                               None, 
                               make_file_id)

        # Verify that the list of file_id's returned by fc1.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.
        self.assertEquals(set(test_file_id_list), set(fc.file_id_list()),
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(test_file_id_list)),
                           repr(sorted(fc.file_id_list()))))

        for i, file_id in enumerate(test_file_id_list):
            filepath = fc.get(file_id)
            # verify that the filepath returned by get is identical to the path
            # that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            fc.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

        # repeat the test with use
        for i, file_id in enumerate(test_file_id_list):
            with fc.use(file_id) as filepath:
                # verify that the file path returned by use is identical to
                # the path that was inserted
                self.assertEquals(filepath, test_file_list[i],
                                  'ERROR: %s != %s' %
                                  (filepath, test_file_list[i]))

                # Verify that the file exists
                self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            self.assertTrue(not os.path.exists(filepath))

        # If we request a bogus file_id, this should raise a logger error
        self.assertRaises(basecontainer.ContainerError,
                          fc.get, 'bogus')

        fc.close()

        # If we ask for a bogus tar file name, the init should throw an IOError
        self.assertRaises(IOError,
                          tarfile_container,
                          'bogus.file', self.testdir, True, 
                          make_file_id)

        # If the output directory does not exist, the init should throw a 
        # ContainerError
        self.assertRaises(basecontainer.ContainerError,
                          tarfile_container,
                          tarfilepath, '/junk/bogus', True, 
                          make_file_id)

    # Test tarfile_container implementations
    def test020_tarfile_container_no_filtering(self):
        test_file_id_list = ['file1', 
                             'file2', 
                             'file3', 
                             'file4', 
                             'file5']
        test_file_list = [os.path.join(self.testdir, f) for f in
                          ['file1.fits', 
                           'file2.fits', 
                           'file3.fits',
                           'file4.fits',
                           'file5.fits']]
        # The gzipped tar file exists so creation of a tarfile_container
        # should succeed.
        # The filterfunc is fitsfilter.
        tarfilepath = os.path.join(self.testdir, 'file5.tar.gz')
        fc = tarfile_container(tarfilepath, 
                               self.testdir, 
                               fitsfilter, 
                               make_file_id)

        # Verify that the list of file_id's returned by fc1.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.
        self.assertEquals(set(test_file_id_list), set(fc.file_id_list()),
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(test_file_id_list)),
                           repr(sorted(fc.file_id_list()))))

        for i, file_id in enumerate(test_file_id_list):
            filepath = fc.get(file_id)
            # verify that the filepath returned by get is identical to the path
            # that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            fc.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

        # repeat the test with use
        for i, file_id in enumerate(test_file_id_list):
            with fc.use(file_id) as filepath:
                # verify that the file path returned by use is identical to
                # the path that was inserted
                self.assertEquals(filepath, test_file_list[i],
                                  'ERROR: %s != %s' %
                                  (filepath, test_file_list[i]))

                # Verify that the file exists
                self.assertTrue(os.path.exists(filepath))

            # Verify that cleanup removes the file
            self.assertTrue(not os.path.exists(filepath))


if __name__ == '__main__':
    unittest.main()

