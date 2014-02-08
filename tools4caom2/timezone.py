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
#*   Script Name:    timezone.py
#*
#*   Purpose:
#+    Module defining tzinfo classes for specific timezones, in particular
#+    the class UTC, a timezone specialized to Coordinated Universal Time.
#+    UTC is a thread-safe singleton, since we never need more than one
#+    of each tzinfo object.
#*
#*   Classes:
#+    UTC   Coordinated Universal Time tzinfo class
#*
#*   Functions:
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
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
