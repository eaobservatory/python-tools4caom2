#!/usr/bin/env python
__author__ = "Russell O. Redman"


import math

from tools4caom2 import __version__

__doc__ = """
Low-accuracy routine to calculate (X, Y, Z) coordinates relative to the center
of the Earth, given a position at (longitude, latitude, elevation).
Observatory locations can be found in the Astronomical Almanace Online,
at http://asa.usno.navy.mil/  in "Section J: Observatory Search".
e.g. JCMT = geolocation(-155.470000,  19.821667, 4198.0)
          = [ -5464385.5627780296, -2493720.1630102545, 2150558.3762761448]
The actual location of the JCMT is
            [ -5461060.909, -2491393.621, 2149257.916 ]

Version: """ + __version__.version

def geolocation(longitude, latitude, elevation_meters):
    """
    Returns a 3-tuple (X,Y,Z) giving the cartesian coordinates relative to the
    center of the Earth in meters.

    longitude : geodetic longitude in decimal degrees
    latitude  : geodetic latitude in decimal degrees
    elevation : above sealevel in meters

    The computed position is only accurate to a few kilometers, which is
    acceptable for most purposes.  Problems with high-accuracy requirements
    should find other routines.

    The equatorial and polar radii are taken from WGRS 80/84
        http://en.wikipedia.org/wiki/World_Geodetic_System
    The formulae for geodetic longitude and latitude are taken from
        http://en.wikipedia.org/wiki/Reference_ellipsoid
    """

    a = 6378137.0
    b = 6356752.3

    cos2oe = (b / a) ** 2
    sin2oe = (a + b) * (a - b) / a ** 2

    theta = math.radians(longitude)
    phi = math.radians(latitude)

    n = a / math.sqrt(1.0 - sin2oe * math.sin(phi) ** 2)
    h = elevation_meters

    return ((n + h) * math.cos(theta) * math.cos(phi),
            (n + h) * math.sin(theta) * math.cos(phi),
            (cos2oe * n + h) * math.sin(phi))
