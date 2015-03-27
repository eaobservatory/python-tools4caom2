from tools4caom2 import __version__

__doc__ = """
The tools4caom2 package is a set of Python modules for Python2.n providing
tools needed to ingest FITS files into CAOM-2 using fits2caom2.

Version: """ + __version__.version

__all__ = [
    'caom2repo_wrapper',
    'geolocation',
    'caom2ingest',
    'mjd',
    'timezone',
]
