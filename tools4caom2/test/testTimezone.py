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
#*   Script Name:    testTimezone.py
#*
#*   Purpose:
#+    Unit test module for the tzinfo class defined in tools4caom2.timezone.
#*
#*   Classes:
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
import os.path
from datetime import datetime, timedelta
import sys
import unittest

from tools4caom2.timezone import UTC


class testUTC(unittest.TestCase):
    def testCreate( self):
        utc = UTC()
        self.assertTrue( utc != None, 'singleton local copy utc is not created')
    
    def testOffset( self):
        utc = UTC()
        self.assertEqual( utc.utcoffset(datetime.now(utc)), timedelta(0))
    
    def testTZName( self):
        utc = UTC()
        self.assertEqual( utc.tzname(datetime.now(utc)), 'UTC')
    
    def testDaylightSavings( self):
        utc = UTC()
        self.assertEqual( utc.dst(datetime.now(utc)), timedelta(0))

if __name__ == '__main__':
    unittest.main()    

