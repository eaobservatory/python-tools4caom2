# Copyright (C) 2023 East Asian Observatory
# All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful,but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,51 Franklin
# Street, Fifth Floor, Boston, MA  02110-1301, USA

try:
    from ConfigParser import SafeConfigParser
except ImportError:
    from configparser import ConfigParser as SafeConfigParser
import os.path

import requests

def cadc_log_in():
    config_file ='~/.tools4caom2/tools4caom2.config'

    config_parser = SafeConfigParser()
    with open(os.path.expanduser(config_file)) as file_:
        config_parser.readfp(file_)

    r = requests.post('https://ws-cadc.canfar.net/ac/login', data={
        'username': config_parser.get('cadc', 'cadc_id'),
        'password': config_parser.get('cadc', 'cadc_key'),
    })

    if r.status_code != 200:
        raise Exception('CADC login request failed')

    return {'CADC_SSO': r.text}
