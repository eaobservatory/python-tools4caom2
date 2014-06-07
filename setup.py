#/*+
#************************************************************************
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#*
#* (c) 2013.                            (c)2013.
#* National Research Council            Conseil national de recherches
#* Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#* All rights reserved                  Tous droits reserves
#*
#* NRC disclaims any warranties,        Le CNRC denie toute garantie
#* expressed, implied, or statu-        enoncee, implicite ou legale,
#* tory, of any kind with respect       de quelque nature que se soit,
#* to the software, including           concernant le logiciel, y com-
#* without limitation any war-          pris sans restriction toute
#* ranty of merchantability or          garantie de valeur marchande
#* fitness for a particular pur-        ou de pertinence pour un usage
#* pose.  NRC shall not be liable       particulier.  Le CNRC ne
#* in any event for any damages,        pourra en aucun cas etre tenu
#* whether direct or indirect,          responsable de tout dommage,
#* special or general, consequen-       direct ou indirect, particul-
#* tial or incidental, arising          ier ou general, accessoire ou
#* from the use of the software.        fortuit, resultant de l'utili-
#*                                      sation du logiciel.
#*
#************************************************************************
#*
#*   Script Name:       setup.py
#*
#*   Purpose:
#*      Distutils setup script for tools4caom2
#*
#*   Functions:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
"""
Distutils setup script for tools4caom2
"""
from setuptools import setup, find_packages
from distutils import debug
import sys

if sys.version_info[0] > 2:
    print 'The tools4caom2 package is only compatible with Python version 2.7'
    sys.exit(-1)

# Uncomment the next line for debugging output
# debug.DEBUG=1

setup(name="tools4caom2",
      version='1.1.4',
      description='Python tools to assist ingestions into CAOM-2, ' + \
                  'especially when using fits2caom2',
      author='Russell Redman',
      author_email='russell.o.redman@gmail.com',
      provides=['tools4caom2'],
      requires=['requests (>=2.3.0)'],
      packages=find_packages(exclude=['*.test']),
      test_suite='tools4caom2.test',
      install_requires = ['distribute']
)
