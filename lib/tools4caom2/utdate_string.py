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

from datetime import datetime
import re

from tools4caom2.__version__ import version

UTDATE_REGEX = r'stamp-(?P<stamp>(?P<stampdate>\d{8})[tT](?P<stamptime>\d{6}))'


def utdate_string(time=datetime.utcnow()):
    """
    Gernates a standardized utc datetime string for inclusion in file names.
    This follows the starlink datetime stamp format:
        yyyymmddThhmmss
    """
    utstr = 'stamp-%d%02d%02dt%02d%02d%02d' % (time.year,
                                               time.month,
                                               time.day,
                                               time.hour,
                                               time.minute,
                                               time.second)

    return utstr
