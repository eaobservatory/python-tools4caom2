#!/usr/bin/env python
#/*+
#************************************************************************
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#*
#* (c) 2011  .                      (c) 2011
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
#*   Script Name:    testLogger.py
#*
#*   Purpose:
#+    Unittest module for the class logger.
#*
#*   Classes:
#+    TestLogger         
#*
#*   Functions:
#*
#*   Field
#*    $Revision: 155 $
#*    $Date: 2012-09-12 16:53:24 -0700 (Wed, 12 Sep 2012) $
#*    $Author: redman $
#*
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
"""
testlogger module, a unittest module that checks whether the tools4caom2.logger 
module, compiles, logs to a disk file, logs to a console file and sends e-mail.
"""
import commands
import os
import os.path
import re
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
        
if __name__ == '__main__':
    unittest.main()    

