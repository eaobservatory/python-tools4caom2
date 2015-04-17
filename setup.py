# Copyright (C) 2014-2015 Science and Technology Facilities Council.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Distutils setup script for tools4caom2
"""
from setuptools import setup, find_packages
from distutils import debug
import sys

if sys.version_info[0] > 2:
    print 'The tools4caom2 package is only compatible with Python version 2.7'
    sys.exit(1)

# Uncomment the next line for debugging output
# debug.DEBUG=1

setup(
    name="tools4caom2",
    version='1.2.4',
    description='Python tools to assist ingestions into CAOM-2, ' +
                'especially when using fits2caom2',
    author='Russell Redman',
    author_email='russell.o.redman@gmail.com',
    provides=['tools4caom2'],
    package_dir={'': 'lib'},
    packages=find_packages(where='lib'),
    scripts=['scripts/autoCert',
             'scripts/cadcdata',
             'scripts/tapquery'],
    test_suite='tools4caom2.test',
    install_requires=['distribute',
                      'requests==2.3.0',
                      'vos',
                      'astropy==0.4.1']
)
