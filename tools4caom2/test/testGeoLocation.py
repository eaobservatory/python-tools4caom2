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
#*   Script Name:    testGeoLocation.py
#*
#*   Purpose:
#*    Testunit module for the function geoLocation
#*
#*   Classes:
#*
#*   Functions:
#+
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
from math import sqrt
import os.path
import sys
import unittest

from tools4caom2.geolocation import geolocation


class testGeoLocation( unittest.TestCase):
    def testExamples( self):
        JCMT = geolocation( -155.470000,  19.821667, 4198.0 )
        JCMTreal = [ -5461060.909, -2491393.621, 2149257.916 ]
        diff = sqrt((float(JCMT[0])-JCMTreal[0])**2 +
                    (float(JCMT[1])-JCMTreal[1])**2 +
                    (float(JCMT[2])-JCMTreal[2])**2)
        allowed_error = 10000.0
        self.assertTrue( diff < allowed_error)


if __name__ == '__main__':
    unittest.main()
    
