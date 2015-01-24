#!/usr/bin/env python2.7
__author__ = "Russell O. Redman"
__version__ = "1.0"


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
import tempfile
import unittest

from tools4caom2.ingest2caom2 import make_file_id
from tools4caom2.ingest2caom2 import fitsfilter
from tools4caom2.basecontainer import basecontainer
from tools4caom2.adfile_container import adfile_container
from tools4caom2.data_web_client import data_web_client
from tools4caom2.logger import logger


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

    # Some extension-dependent headers
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


class testAdfileContainer(unittest.TestCase):
    """
    unit tests for tools4caom2.adfile_container class
    """
    def setUp(self):
        """
        Create a set of fits files (file6, file7, file8) and store them
        in an adfile container.  Include a non-FITS file file9.txt to verify
        that filtering works as intended.
        """
        # save the argument vector
        self.argv = sys.argv

        # set up the test envirnonment
        self.testdir = tempfile.mkdtemp()
        self.log = logger(os.path.join(self.testdir, 'testadfile.log'),
                          console_output=False)

        self.dataweb = data_web_client(self.testdir, self.log)

        # fake data
        fakedata = numpy.arange(10)

        # Create fits files with suitable test headers
        # Files file[1-4].fits will be present in the working directory
        # File file5.fits will be in file5.tar.gz.
        # Files file[6-8].fits will be in file6.ad.
        # There are also some garbage files to be ignored.

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

        TEXT = open(os.path.join(self.testdir, 'file9.txt'), 'w')
        print >>TEXT, "This is some text"
        TEXT.close()

        # Create an ad file containing file6.fits, file7.fits and file8.fits
        # and file9.txt, i.e. no filtering in the AD file itself.
        # Save these files in the subdirectory 'save'
        self.savedir = os.path.join(self.testdir, 'save')
        os.mkdir(self.savedir)
        ADFILE = open(os.path.join(self.testdir, 'file6.ad'), 'w')
        for f in ['file6.fits', 'file7.fits', 'file8.fits', 'file9.txt']:
            filepath = os.path.join(self.testdir, f)
            file_id = make_file_id(f)

            ok = self.dataweb.put(filepath, 'TEST', file_id, adstream='test')
            if not ok:
                raise RuntimeError(cmd + ': ' + output,
                                   logging.ERROR)
            os.rename(filepath, os.path.join(self.savedir, f))

            print >>ADFILE, 'ad:TEST/' + file_id
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

    def test010_adfile_container_no_filter(self):
        """
        Test adfile_container implementations with no filtering
        """
        test_list = ['file6', 'file7', 'file8', 'file9']
        adfilepath = os.path.join(self.testdir, 'file6.ad')

        # Verify that the files are in ad
        for fid in test_list:
            headers = self.dataweb.info('TEST', fid)
            if not headers:
                self.log.console('ERROR: file not in ad: ' + fid +
                                 ': ' + output,
                                 logging.ERROR)

        # Make a subdirectory in testdir to hold the files from ad for
        # comparison with the originals
        workdir = os.path.join(self.testdir, 'work')
        os.mkdir(workdir)
        test_file_list = [os.path.join(workdir, f) for f in
                          ['file6.fits',
                           'file7.fits',
                           'file8.fits',
                           'file9.txt']]

        # These files should all exist in ad so creation of an
        #  adfile_container should succeed.
        # The filetrfunc in this test is None.
        fc = adfile_container(adfilepath,
                              workdir,
                              None)

        # Verify that the list of file_id's returned by fc1.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.  File9 should be present in this list,
        # even though it is not a FITS file.
        self.assertEquals(set(test_list), set(fc.file_id_list()),
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(test_list)),
                           repr(sorted(fc.file_id_list()))))

        for i, file_id in enumerate(test_list):
            filepath = fc.get(file_id)
            # verify that the file path returned by get is identical to the
            # path that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # verify that the file is identical to its original
            self.assertTrue(filecmp.cmp(os.path.join(self.savedir,
                                                     test_file_list[i]),
                                        filepath))

            # Verify that cleanup removes the file
            fc.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

    def test020_adfile_container(self):
        """
        Test adfile_container implementations with FITS filtering
        """
        test_list = ['file6', 'file7', 'file8']
        adfilepath = os.path.join(self.testdir, 'file6.ad')

        # Verify that the files are in ad
        for fid in test_list:
            headers = self.dataweb.info('TEST', fid)
            if not headers:
                self.log.console('ERROR: file not in ad: ' + fid +
                                 ': ' + output,
                                 logging.ERROR)

        # Make a subdirectory in testdir to hold the files from ad for
        # comparison with the originals
        workdir = os.path.join(self.testdir, 'work')
        os.mkdir(workdir)
        test_file_list = [os.path.join(workdir, f) for f in
                          ['file6.fits',
                           'file7.fits',
                           'file8.fits']]

        # These files should all exist in ad so creation of an
        #  adfile_container should succeed.
        # the filterfunc in this test is tools4caom2.ingest2caom2.fitsfilter
        fc = adfile_container(adfilepath,
                              workdir,
                              fitsfilter)

        # Verify that the list of file_id's returned by fc1.file_id_list()
        # is identical to the list we inserted.  Beware that the order is
        # undefined, so compare sets.  File9 should be present in this list,
        # even though it is not a FITS file.
        self.assertEquals(set(test_list), set(fc.file_id_list()),
                          'file_id_list sets are not equal %s != %s' %
                          (repr(sorted(test_list)),
                           repr(sorted(fc.file_id_list()))))

        for i, file_id in enumerate(test_list):
            filepath = fc.get(file_id)
            # verify that the file path returned by get is identical to the
            # path that was inserted
            self.assertEquals(filepath, test_file_list[i],
                              'ERROR: %s != %s' %
                              (filepath, test_file_list[i]))

            # Verify that the file exists
            self.assertTrue(os.path.exists(filepath))

            # verify that the file is identical to its original
            self.assertTrue(filecmp.cmp(os.path.join(self.savedir,
                                                     test_file_list[i]),
                                        filepath))

            # Verify that cleanup removes the file
            fc.cleanup(file_id)
            self.assertTrue(not os.path.exists(filepath))

    def test020_adfile_container(self):
        """
        Test adfile_container implementations with FITS filtering
        """
        test_list = ['file6', 'file7', 'file8']
        adfilepath = os.path.join(self.testdir, 'file6.ad')

        # Verify that the files are in ad
        for fid in test_list:
            headers = self.dataweb.info('TEST', fid)
            if headers:
                self.log.console('ERROR: file not in ad: ' + fid +
                                 ': ' + output,
                                 logging.ERROR)

        # Make a subdirectory in testdir to hold the files from ad for
        # comparison with the originals
        workdir = os.path.join(self.testdir, 'work')
        os.mkdir(workdir)
        test_file_list = [os.path.join(workdir, f) for f in
                          ['file6.fits',
                           'file7.fits',
                           'file8.fits']]

        # These files should all exist in ad so creation of an
        #  adfile_container should succeed.
        # the filterfunc in this test is tools4caom2.ingest2caom2.fitsfilter
        fc = adfile_container(adfilepath,
                              workdir,
                              fitsfilter)

        # run the test with use
        for i, file_id in enumerate(test_list):
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

        # If we ask for an adfile name with a different extension,
        # the init should throw a LoggerError
        self.assertRaises(logger.LoggerError,
                          adfile_container,
                          'bogus.file',
                          self.testdir,
                          None)

        # If the output directory does not exist, the init should throw a
        # LoggerError
        self.assertRaises(logger.LoggerError,
                          adfile_container,
                          adfilepath,
                          '/junk/bogus',
                          None)

        # If we request a bogus file_id, this should raise a
        # LoggerError
        self.assertRaises(logger.LoggerError,
                          fc.get, 'bogus')


if __name__ == '__main__':
    unittest.main()
