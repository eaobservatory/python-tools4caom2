__author__ = "Russell O. Redman"

from datetime import datetime, timedelta
from pytz import UTC
import re

from tools4caom2 import __version__

__doc__ = """
Conversion routines to and from Modified Julian Dates for UTC and strings.
Beware that calculations of MJD based on UTC are intrinsically inaccurate
at the level od ~1 second (of time).  Higher accuracy would require the
calculation of UT1 or one of the other variations of UT that must be specified.

Like most system time libraries, this routine ignores leap seconds.

version: """ + __version__.version

ORIGIN = datetime(1858, 11, 17, 0, 0, 0, 0, tzinfo=UTC)
OFFSET = 2400000.5


def utc2mjd(dt):
    """
    Convert a UTC datetime dt to MJD.
    If dt is not timezone-aware, assume it is already in UTC.

    Arguments:
    dt:      a Python datetime
    """
    if dt.tzinfo is None:
        dtdelta = dt.replace(tzinfo=UTC) - ORIGIN
    else:
        dtdelta = dt - ORIGIN

    return (dtdelta.days * 1.0 +
            (dtdelta.seconds + dtdelta.microseconds / 1000000.0) / 86400.0)


def mjd2utc(mjd):
    """
    Convert an MJD to a UTC datetime.

    Arguments:
    mjd:     a Modified Julian Date
    """
    return ORIGIN + timedelta(seconds=mjd * 86400)


def str2mjd(dt_string, format='%Y-%m-%dT%H:%M:%S'):
    """
    Convert a string containing a datetime to MJD, accurate to seconds.

    Arguments:
    dt_string: a datetime written as a string
    format:    the format needed to read the datetime
    """
    # Strip off trailing fractions of a second.
    if re.match(r'[^\d]*(\d{1,4}-\d{2}-\d{2})[ Tt](\d{2}:\d{2}:\d{2}).*',
                dt_string):
        dt = re.sub(r'[^\d]*(\d{1,4}-\d{2}-\d{2})[ Tt](\d{2}:\d{2}:\d{2}).*',
                    r'\1T\2', dt_string)
    elif re.match(r'[^\d]*(\d{1,4}-\d{2}-\d{2}).*', dt_string):
        dt = re.sub(r'[^\d]*(\d{1,4}-\d{2}-\d{2}).*', r'\1T00:00:00',
                    dt_string)
    else:
        raise ValueError('the string "%s" does not match a date'
                         'or datetime format' % (dt_string))

    dtc = datetime.strptime(dt, format).replace(tzinfo=UTC)

    dtdiff = dtc - ORIGIN
    mjd = (dtdiff.days * 1.0 +
           (dtdiff.seconds + dtdiff.microseconds / 1000000.0) / 86400.0)

    return mjd


def mjd2str(mjd):
    """
    Convert an MJD to an ISO 8601 string.

    Arguments:
    mjd:     a Modified Julian Date
    """
    return mjd2utc(mjd).isoformat()
