python-tools4caom2
==================

A set of Python tools useful when developing ingestion code for CAOM-2 archives
at the CADC (and more generally as well).  These are contained in the package
tools4caom2/

caom2repo_wrapper.py
    wrapper for CAOM-2 repository tool (get, put, update, remove)
geolocation.py
    calculate X,Y,Z positions given long,lat,elev
fits2caom2.py
    utility functions for interacting with fits2caom2
logger.py
    class implementing an enhanced logger
mjd.py
    function for MJD conversions
