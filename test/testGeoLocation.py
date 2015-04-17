# Copyright (C) 2014 Science and Technology Facilities Council.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from math import sqrt
import os.path
import sys
import unittest

from tools4caom2.geolocation import geolocation


class testGeoLocation(unittest.TestCase):
    def testExamples(self):
        JCMT = geolocation(-155.470000,  19.821667, 4198.0)
        JCMTreal = [-5461060.909, -2491393.621, 2149257.916]
        diff = sqrt((float(JCMT[0]) - JCMTreal[0])**2 +
                    (float(JCMT[1]) - JCMTreal[1])**2 +
                    (float(JCMT[2]) - JCMTreal[2])**2)
        allowed_error = 10000.0
        self.assertTrue(diff < allowed_error)


if __name__ == '__main__':
    unittest.main()
