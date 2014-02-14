#!/usr/bin/env python2.7
"""
testlogger module, a unittest module that checks whether the tools4caom2.logger 
module, compiles, logs to a disk file, logs to a console file and sends e-mail.
"""
import commands
import os
import os.path
import re
import subprocess
import string
import sys
import tempfile
import unittest

from tools4caom2.logger import logger


class TestLogger(unittest.TestCase):
    def setUp(self):
        self.logfile = tempfile.mktemp('.log')

    def tearDown(self):
        pass
        #os.remove(self.logfile)
        
    def test010_logfile(self):
        """
        Writes to the log file and reads back the value.
        """
        self.log = logger(self.logfile, console_output=False)
        self.log.file('test message')
        self.log.console('console message')
        
        self.assertTrue(os.path.exists( self.logfile),
                        'Could not find log file %s' % (self.logfile))
        LOG = open(self.logfile, 'r')
        out = LOG.read()
        LOG.close()
        self.assertTrue(string.find(out, 'test message') > -1, 
                        'Could not find "test message" in log file')
        self.assertTrue(string.find( out, 'console message') > -1, 
                        'Could not find "console message" in log file')

    def test020_email(self):
        """
        Writes to the log file and sends a message to the test user
        """
        self.log = logger(self.logfile,
                          console_output=False,
                          to=['test.nulldevice@mailer.hia.nrc.ca'],
                          sender='test.nulldevice@mailer.hia.nrc.ca',
                          subject='test message',
                          smtphost='mailer.hia.nrc.ca')
        self.log.console('console message')
        self.log.file('file message - should not appear')
        self.assertEquals('console message\n', self.log.text)
        self.log.send_email()
        self.assertEquals('', self.log.text)

    def test030_setter_getter(self):
        """
        Sets and gets the text buffer.  Note that the same logger is used by
        all these tests and that setting the text buffer means the contents
        of the text buffer will be different from the contents of the log file.
        """
        self.log = logger(self.logfile, console_output=False)
        
        messagelist = ['line one',
                       'line two',
                       'line three']
        for m in messagelist:
            self.log.set_text(m)
            self.log.console('\ntest message')
            
            mafter = self.log.get_text()
            self.assertTrue(re.match(m + r'\n.*test message', mafter),
                            'text buffer does not begin with "' + m +
                            '" and end with "test message":"' + mafter + '"')

    def test040_stderr(self):
        """
        Run the clientLogger.py program to verify that all commands are written
        to the standard error stream and that setting the logevel filters the
        correct set of messages.
        """
        clientpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                  'clientLogger.py')
        
        clientCmd = clientpath + ' --log=' + self.logfile
        po = subprocess.Popen(clientCmd, 
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        (stdo, stde) = po.communicate()
        self.assertTrue(len(stdo) == 0,
                        'stdoutput should be empty by held "' + stdo + '"')
        self.assertTrue(re.search('MESSAGE1', stde),
                        'Running "' + clientCmd + '", '
                        'MESSAGE1 was not found in stderr')
        self.assertFalse(re.search('DEBUG1', stde),
                        'Running "' + clientCmd + '", '
                        'DEBUG1 was found in stderr')
        self.assertTrue(re.search('INFO1', stde),
                        'Running "' + clientCmd + '", '
                        'INFO1 was not found in stderr')
        self.assertTrue(re.search('WARNING1', stde),
                        'Running "' + clientCmd + '", '
                        'WARNING1 was not found in stderr')
                
        clientCmd = (clientpath + ' --log=' + self.logfile
                    + ' --console_output=False')
        po = subprocess.Popen(clientCmd, 
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        (stdo, stde) = po.communicate()
        self.assertTrue(len(stdo) == 0,
                        'stdoutput should be empty by held "' + stdo + '"')
        self.assertFalse(re.search('MESSAGE1', stde),
                         'Running "' + clientCmd + '", '
                         'MESSAGE1 was found in stderr')
        self.assertFalse(re.search('DEBUG1', stde),
                         'Running "' + clientCmd + '", '
                         'DEBUG1 was found in stderr')
        self.assertFalse(re.search('INFO1', stde),
                         'Running "' + clientCmd + '", '
                         'INFO1 was found in stderr')
        self.assertFalse(re.search('WARNING1', stde),
                         'Running "' + clientCmd + '", '
                         'WARNING1 was found in stderr')

        clientCmd = (clientpath + ' --log=' + self.logfile
                     + ' --loglevel=debug')
        po = subprocess.Popen(clientCmd, 
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        (stdo, stde) = po.communicate()
        self.assertTrue(len(stdo) == 0,
                        'stdoutput should be empty by held "' + stdo + '"')
        self.assertTrue(re.search('MESSAGE1', stde),
                        'Running "' + clientCmd + '", '
                        'MESSAGE1 was not found in stderr')
        self.assertTrue(re.search('DEBUG1', stde),
                       'Running "' + clientCmd + '", '
                       'DEBUG1 was not found in stderr')
        self.assertTrue(re.search('INFO1', stde),
                        'Running "' + clientCmd + '", '
                        'INFO1 was not found in stderr')
        self.assertTrue(re.search('WARNING1', stde),
                        'Running "' + clientCmd + '", '
                        'WARNING1 was not found in stderr')
                
        clientCmd = (clientpath + ' --log=' + self.logfile
                     + ' --loglevel=warn')
        po = subprocess.Popen(clientCmd, 
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        (stdo, stde) = po.communicate()
        self.assertTrue(len(stdo) == 0,
                        'stdoutput should be empty by held "' + stdo + '"')
        self.assertFalse(re.search('MESSAGE1', stde),
                         'Running "' + clientCmd + '", '
                         'MESSAGE1 was found in stderr')
        self.assertFalse(re.search('DEBUG1', stde),
                        'Running "' + clientCmd + '", '
                        'DEBUG1 was found in stderr')
        self.assertFalse(re.search('INFO1', stde),
                         'Running "' + clientCmd + '", '
                         'INFO1 was found in stderr')
        self.assertTrue(re.search('WARNING1', stde),
                        'Running "' + clientCmd + '", '
                        'WARNING1 was not found in stderr')
                
                
if __name__ == '__main__':
    unittest.main()    

