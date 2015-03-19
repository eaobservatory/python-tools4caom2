python-tools4caom2
==================

A set of Python tools useful when developing ingestion code for CAOM-2 archives
at the CADC (and more generally as well).  These are contained in the package
tools4caom2/

basecontainer.py
    base class for "containers" describing sets of files
adfile_container.py
    class for sets of files in CADC AD (archive directory)
filelist_container.py
    class for set of files on disk (including in a directory)
tarfile_container.py
    class for set of files in a tarfile

caom2repo_wrapper.py
    wrapper for CAOM-2 repository tool (get, put, update, remove)
geolocation.py
    calculate X,Y,Z positions given long,lat,elev
caom2ingest.py
    base class for code to ingest FITS files into CAOM-2
fits2caom2.py
    utility functions for interacting with fits2caom2
logger.py
    class implementing an enhanced logger
mjd.py
    function for MJD conversions
timezone.py
    trivial classes for UTC
