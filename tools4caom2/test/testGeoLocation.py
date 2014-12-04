#!/usr/bin/env python2.7
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
    
