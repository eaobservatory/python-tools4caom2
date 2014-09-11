#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
from datetime import datetime
import re

from tools4caom2.__version__ import version

UTDATE_REGEX = r'stamp-(?P<stamp>(?P<stampdate>\d{8})[tT](?P<stamptime>\d{6}))'

def utdate_string(time=datetime.utcnow()):
    """
    Gernates a standardized utc datetime string for inclusion in file names.
    This follows the starlink datetime stamp format:
        yyyymmddThhmmss
    """
    utstr = 'stamp-%d%02d%02dt%02d%02d%02d' % (time.year,
                                               time.month,
                                               time.day,
                                               time.hour,
                                               time.minute,
                                               time.second)

    return utstr
    