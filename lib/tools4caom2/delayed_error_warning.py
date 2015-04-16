__author__ = "Russell O. Redman"

import argparse
from contextlib import contextmanager
from collections import OrderedDict
import datetime
import logging
import os
import os.path


from tools4caom2.data_web_client import data_web_client
from tools4caom2.error import CAOMError
from tools4caom2.validation import CAOMValidation, CAOMValidationError
from tools4caom2.__version__ import version as tools4caom2version

logger = logging.getLogger(__name__)


# *****************************************************************************
# A set of classes that allows error and warning messages to be recorded
# without triggering an immediate exit.  Errors and warnings are logged as
# ERROR and WARNING messages, but all errors are trapped and a final call to
# sys.exit made if any errors are reported.
# *****************************************************************************
class delayed_error_warning(object):
    """
    A delayed_error_warning examines files for problems and records bad files
    in dictionaries of errors and warnings.
    """
    def __init__(self,
                 outdir,
                 archive,
                 fileid_regex_dict,
                 make_file_id,
                 debug=False):
        """
        A delayed_error_warning is a class that examines files for possible
        problems.  If problems are found, the offending filename is recorded
        in the dictionary errors.
        """
        self.errors = {}
        self.warnings = {}

        self.validation = CAOMValidation(outdir, archive, fileid_regex_dict,
                                         make_file_id)

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

    def warning_count(self):
        """
        Return number of files for which errors were reported
        """
        return len(self.warnings)

    def error(self, filename, errormsg):
        """
        Record this file as having an error

        Arguments:
        filename  : file name to examine
        errormsg  : error message to report
        """
        logger.info('delayed_error_warning: error for filename %s', filename)
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
        logger.info('delayed_error_warning: warning for filename %s', filename)
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
            logger.info('ERRORS and WARNINGS')
            filelist = sorted(filelist)
            for filename in filelist:
                logger.info('')
                logger.info(filename)
                if filename in self.errors:
                    for errormsg in self.errors[filename]:
                        logger.error(errormsg)
                if filename in self.warnings:
                    for warningmsg in self.warnings[filename]:
                        logger.warning(warningmsg)

    # ***********************************************************************
    # The following methods test for individual conditions.  Each test will
    # return True if the file is acceptable for ingestion.
    #
    # To test more/different conditions, derive a new class from
    # delayed_error_warning and add the new tests to the derived class.
    #
    # Arbitrary messages can also be added by calling error or warning
    # directly, leaving the logic of the test external to the class.
    # **********************************************************************
    def sizecheck(self, filename):
        """
        Arguments:
        filename : filesystem path or VOspace uri of a file

        Returns True if the file has a non-zero length, False otherwise
        """
        logger.info('delayed_error_warning: sizecheck for filename %s',
                    filename)

        try:
            self.validation.check_size(filename)

        except CAOMValidationError:
            self.error(filename, 'file has length = 0')
            return False

        return True

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
        logger.info('delayed_error_warning: namecheck for filename %s',
                    filename)

        try:
            self.validation.check_name(filename)

        except CAOMValidationError:
            if report:
                self.error(filename, 'namecheck failed')
            return False

        return True

    def in_archive(self, filename, severity_dict):
        """
        Check whether a file is or is not present in the archive

        Arguments:
        filename      : filesystem path or VOspace uri of a file
        severity_dict : dictionary telling whether it is an error, warning or
                        acceptable for the file to be present (True) or absent
                        (False) in the archive.
        """
        logger.info('delayed_error_warning: in_archive for filename %s',
                    filename)
        ok = False
        try:
            self.validation.is_in_archive(filename)
        except CAOMValidationError:
            if severity_dict[False] == 'error':
                self.error(filename,
                           'expected file not found in the archive')
            elif severity_dict[False] == 'warning':
                ok = True
                self.warning(filename,
                             'file is not present in the archive')
            else:
                ok = True
        else:
            if severity_dict[True] == 'error':
                self.error(filename,
                           'name conflict with existing file in the archive')
            elif severity_dict[True] == 'warning':
                ok = True
                self.warning(filename,
                             'existing file has this name in the archive')
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
        logger.info('delayed_error_warning: fitsverify for filename %s',
                    filename)

        try:
            self.validation.verify_fits(filename)
        except CAOMValidationError:
            self.error(filename, 'fitsverify reported errors')
            return False

        return True

    def expect_keyword(self, filename, key, header, mandatory=False):
        """
        Return True if the mandatory key is defined in header, False otherwise

        Arguments:
        filename : filesystem path to a file
        header   : FITS header from the primary HDU
        key      : mandatory keyword
        """
        logger.info(
            'delayed_error_warning: expect_keyword %s for filename %s',
            key, filename)

        try:
            self.validation.expect_keyword(filename, key, header)

        except CAOMValidationError:
            if mandatory:
                self.error(filename,
                           'mandatory keyword "' + key +
                           '" is missing or undefined')
            else:
                self.warning(filename,
                             'expected keyword "' + key +
                             '" is missing or undefined')

            return False

        return True

    def restricted_value(self, filename, key, header, value_list):
        """
        Return True if the header[key] in value_list, False otherwise

        Arguments:
        filename   : filesystem path to a file
        key        : keyword whose value must be in the value_list
        header     : FITS header from the primary HDU
        value_list : list of acceptable values
        """
        logger.info(
            'delayed_error_warning: restricted_value for %s in filename %s',
            key, filename)
        ok = False

        try:
            self.validation.restricted_value(filename, key, header, value_list)

        except CAOMValidationError:
            self.error(filename,
                       'keyword "' + key + '" with a restricted set of values '
                       'is missing, undefined or not in ' + repr(value_list))

            return False

        return True
