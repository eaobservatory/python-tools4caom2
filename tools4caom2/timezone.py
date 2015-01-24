#!/usr/bin/env python
import datetime
import thread

from tools4caom2 import __version__

__doc__ = """
The timezone module defines the UTC class as a thread-safe singleton timezone 
object.

Version: """ + __version__.version
    

#************************************************************************
# A datetime.tzinfo object for UTC
#************************************************************************
class UTC(datetime.tzinfo):
    """
    UTC is a thread-safe tzinfo class for Coordinated Universal Time
    """
    
    utc = None

    class _UTC_impl_(datetime.tzinfo):
        def utcoffset(self, dt):
            return datetime.timedelta(0)

        def tzname(self, dt):
            return 'UTC'

        def dst(self, dt):
            return datetime.timedelta(0)

    def __init__(self):
        """
        Initialize the UTC tzinfo singleton if necessary
        
        Arguments:
        <None>
        """
        if UTC.utc == None:
            mylock = thread.allocate_lock()
            if mylock.acquire():
                # Check again in case some other thread acquired the lock first
                if UTC.utc == None:
                    UTC.utc = UTC._UTC_impl_()
            if mylock.locked():
                mylock.release()

    def utcoffset(self, dt):
        """
        return the utcoffset (i.e. 0) from the singleton
    
        Arguments:
        dt:      a datetime
        """
        return UTC.utc.utcoffset(dt)

    def tzname(self, dt):
        """
        return the tzname from the singleton
    
        Arguments:
        dt:      a datetime
        """
        return UTC.utc.tzname(dt)

    def dst(self, dt):
        """
        return the daylight savings time offset using the singleton
    
        Arguments:
        dt:      a datetime
        """
        return UTC.utc.dst(dt)
