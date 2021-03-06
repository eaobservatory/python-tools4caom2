# Copyright (C) 2014 Science and Technology Facilities Council.
# Copyright (C) 2015 East Asian Observatory.
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


from datetime import datetime, timedelta
import os.path
from pytz import UTC
import sys
import unittest

from tools4caom2.mjd import utc2mjd, mjd2utc, str2mjd, mjd2str


class testMJDConversions(unittest.TestCase):
    def testUTC_StartOfYear(self):
        for (year, value) in ((2000, 51544.0),
                              (2005, 53371.0),
                              (2010, 55197.0),
                              (2015, 57023.0),
                              (2020, 58849.0)):
            self.assertEqual(utc2mjd(datetime(year, 1, 1, tzinfo=UTC)), value)

    def testUTC_DOY(self):
        start2010 = datetime(2010, 1, 1, tzinfo=UTC)
        for doy in range(365):
            self.assertEqual(utc2mjd(start2010 + timedelta(days=doy)),
                             55197.0 + doy)

    def testUTC_ToFrom(self):
        start2010 = datetime(2010, 1, 1, tzinfo=UTC)
        for doy in range(365):
            utin = start2010 + timedelta(days=doy)
            mjd = utc2mjd(utin)
            utout = mjd2utc(mjd)
            self.assertEqual(utin, utout)

    def testUTC_FractionsOfDays(self):
        start2010 = datetime(2010, 1, 1, tzinfo=UTC)
        for minutes in range(24 * 60):
            utin = start2010 + timedelta(minutes=minutes)
            mjd = utc2mjd(utin)
            utout = mjd2utc(mjd)
            self.assertEqual(utin, utout)

    def testSTR_StartOfYear(self):
        for (year, value) in (('2000-01-01T00:00:00', 51544.0),
                              ('2005-01-01T00:00:00', 53371.0),
                              ('2010-01-01T00:00:00', 55197.0),
                              ('2015-01-01T00:00:00', 57023.0),
                              ('2020-01-01T00:00:00', 58849.0),
                              ('2020-01-01T00:00:00.000', 58849.0)):
            self.assertEqual(str2mjd(year), value)

    def testSTR_DOY(self):
        dateformat = '2010-%02d-%02dT00:00:00'
        mjd0 = 55197.0
        for month in range(12):
            days = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
            for day in range(days[month]):
                datestr = dateformat % (month + 1, day + 1)
                self.assertEqual(str2mjd(datestr), mjd0 + day)
            mjd0 += days[month]

    def testSTR_ToFrom(self):
        dateformat = '2010-%02d-%02dT00:00:00'
        for month in range(12):
            days = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
            for day in range(days[month]):
                datein = dateformat % (month+1, day+1)
                mjd = str2mjd(datein)
                dateout = mjd2str(mjd)
                if len(dateout) > len(datein):
                    dateout = dateout[:len(datein)]
                self.assertEqual(datein, dateout)

    def testSTR_FractionsOfDays(self):
        dateformat = '2010-01-01T%02d:%02d:00'
        for hour in range(24):
            for minute in range(60):
                datein = dateformat % (hour, minute)
                mjd = str2mjd(datein)
                dateout = mjd2str(mjd)
                if len(dateout) > len(datein):
                    dateout = dateout[:len(datein)]
                self.assertEqual(datein, dateout)

    def testSTR_BadStrings(self):
        bad = ('2000-00-01T00:00:00',
               '2000-13-01T00:00:00',
               '2000-01-00T00:00:00',
               '2000-01-32T00:00:00',
               '2000-01-01T24:00:00',
               '2000-01-01T00:60:00',
               '2000-01-01T00:00:60',
               'bogus_string')
        for s in bad:
            self.assertRaises(ValueError, str2mjd, s)
