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
#*   Script Name:    __init__.py (makes ingest2caom2 into a package)
#*
#*   Purpose:
#+     makes ingest2caom2 into a package
#*
#*   Classes:
#*
#*   Functions:
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
from tools4caom2 import __version__

__doc__ = """
The tools4caom2 package is a set of Python modules for Python2.n providing
tools needed to ingest FITS files into CAOM-2 using fits2caom2.

Version: """ + __version__.version

__all__ = ['adfile_container',
           'basecontainer',
           'caom2repo_wrapper',
           'database',
           'dataproc_container',
           'filelist_container',
           'geolocation',
           'gridengine',
           'ingest2caom2',
           'logger',
           'mjd',
           'tarfile_container',
           'timezone']
