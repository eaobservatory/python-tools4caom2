#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import sys
import unittest

from tools4caom2.timezone import UTC


class testUTC(unittest.TestCase):
    def testCreate(self):
        utc = UTC()
        self.assertTrue(utc is not None,
                        'singleton local copy utc is not created')

    def testOffset(self):
        utc = UTC()
        self.assertEqual(utc.utcoffset(datetime.now(utc)), timedelta(0))

    def testTZName(self):
        utc = UTC()
        self.assertEqual(utc.tzname(datetime.now(utc)), 'UTC')

    def testDaylightSavings(self):
        utc = UTC()
        self.assertEqual(utc.dst(datetime.now(utc)), timedelta(0))


if __name__ == '__main__':
    unittest.main()
