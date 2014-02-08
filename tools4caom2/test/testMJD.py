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
#*   Script Name:    testMJD.py
#*
#*   Purpose:
#+    test conversions to and from Modified Julian Days (MJDs)
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
from datetime import datetime, timedelta
import os.path
import sys
import unittest

from tools4caom2.mjd import utc2mjd, mjd2utc, str2mjd, mjd2str
from tools4caom2.timezone import UTC


class testMJDConversions( unittest.TestCase):
    def testUTC_StartOfYear( self):
        utc = UTC()
        for (year, value) in ((2000,51544.0),
                              (2005,53371.0),
                              (2010,55197.0),
                              (2015,57023.0),
                              (2020,58849.0)):
            self.assertEqual( utc2mjd(datetime(year,1,1,tzinfo=utc)), value)
    
    def testUTC_DOY( self):
        utc = UTC()
        start2010 = datetime(2010,1,1,tzinfo=utc)
        for doy in range(365):
            self.assertEqual( utc2mjd(start2010+timedelta(days=doy)), 55197.0+doy)
        
    def testUTC_ToFrom( self):
        start2010 = datetime(2010,1,1,tzinfo=UTC())
        for doy in range(365):
            utin = start2010+timedelta(days=doy)
            mjd = utc2mjd( utin)
            utout = mjd2utc( mjd)
            self.assertEqual(  utin, utout)

    def testUTC_FractionsOfDays( self):
        start2010 = datetime(2010,1,1,tzinfo=UTC())
        for minutes in range(24*60):
            utin = start2010+timedelta(minutes=minutes)
            mjd = utc2mjd( utin)
            utout = mjd2utc( mjd)
            self.assertEqual(  utin, utout)

    def testSTR_StartOfYear( self):
        utc = UTC()
        for (year, value) in (('2000-01-01T00:00:00',51544.0),
                              ('2005-01-01T00:00:00',53371.0),
                              ('2010-01-01T00:00:00',55197.0),
                              ('2015-01-01T00:00:00',57023.0),
                              ('2020-01-01T00:00:00',58849.0),
                              ('2020-01-01T00:00:00.000',58849.0)):
            self.assertEqual( str2mjd(year), value)
    
    def testSTR_DOY( self):
        dateformat = '2010-%02d-%02dT00:00:00'
        mjd0 = 55197.0
        for month in range(12):
            days = (31,28,31,30,31,30,31,31,30,31,30,31)
            for day in range(days[month]):
                datestr = dateformat % (month+1, day+1)
                self.assertEqual( str2mjd(datestr), mjd0+day)
            mjd0 += days[month]
        
    def testSTR_ToFrom( self):
        dateformat = '2010-%02d-%02dT00:00:00'
        for month in range(12):
            days = (31,28,31,30,31,30,31,31,30,31,30,31)
            for day in range(days[month]):
                datein = dateformat % (month+1, day+1)
                mjd = str2mjd(datein)
                dateout = mjd2str(mjd)
                if len(dateout) > len(datein):
                    dateout = dateout[:len(datein)]
                self.assertEqual( datein, dateout)

    def testSTR_FractionsOfDays( self):
        dateformat = '2010-01-01T%02d:%02d:00'
        for hour in range(24):
            for minute in range(60):
                datein = dateformat % (hour, minute)
                mjd = str2mjd(datein)
                dateout = mjd2str(mjd)
                if len(dateout) > len(datein):
                    dateout = dateout[:len(datein)]
                self.assertEqual( datein, dateout)

    def testSTR_BadStrings( self):
        bad = ('2000-00-01T00:00:00',
               '2000-13-01T00:00:00',
               '2000-01-00T00:00:00',
               '2000-01-32T00:00:00',
               '2000-01-01T24:00:00',
               '2000-01-01T00:60:00',
               '2000-01-01T00:00:60',
               'bogus_string')
        for s in bad:
            self.assertRaises( ValueError, str2mjd, s)

if __name__ == '__main__':
    unittest.main()    

