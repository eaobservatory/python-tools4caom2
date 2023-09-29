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

__author__ = "Russell O. Redman"

import argparse
try:
    from ConfigParser import SafeConfigParser
except ImportError:
    from configparser import ConfigParser as SafeConfigParser
import logging
import netrc
import os.path
import subprocess
import sys

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth


logger = logging.getLogger(__name__)


def renew(proxypath, username, passwd, daysvalid):
    """
    Renew the proxy certificate

    Arguments:
    proxypath: path to the proxy certificate (pem) file
    username:  username to use for
    """
    certHost = 'https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca'
    certQuery = "/cred/proxyCert?daysValid="

    url = ''.join([certHost, certQuery, str(daysvalid)])
    logger.debug('Request URL: %s', url)

    try:
        r = requests.get(
            url,
            auth=HTTPBasicAuth(username, passwd))

        r.raise_for_status()

        with open(proxypath, 'wb') as w:
            for buf in r.iter_content(chunk_size=128):
                w.write(buf)

        r.close()

    except HTTPError as e:
        logger.exception('HTTP error getting certificate')
        # logger.error('Request headers: %r', e.request.headers)
        logger.error('Response: %r', e.response.content)
        logger.error('Response headers: %r', e.response.headers)

    except:
        logger.exception('Failed to get certificate')


def run():
    """
    Auto-renew CADC proxy certificate using credentials from either the
    user config file or from .netrc.
    """
    ap = argparse.ArgumentParser('autoCert',
                                 fromfile_prefix_chars='@')
    ap.add_argument('--proxy',
                    default='$HOME/.ssl/cadcproxy.pem',
                    help='path to CADC proxy')
    ap.add_argument('--userconfig',
                    default='$HOME/.tools4caom2/tools4caom2.config',
                    help='path to user configuration file')
    ap.add_argument('--daysvalid',
                    default=7,
                    type=int,
                    help='days for which the certificate will remain valid')
    ap.add_argument('--minvalid',
                    default=5,
                    type=int,
                    help='minimum days for which the certificate should '
                         'remain valid')
    ap.add_argument('-v', '--verbose',
                    action='store_true',
                    help='output extra information')
    a = ap.parse_args()

    logging.basicConfig(level=(logging.DEBUG if a.verbose else logging.INFO))

    minvalid = min(a.minvalid, a.daysvalid)
    secvalid = str(86400*minvalid)

    cadcproxy = os.path.abspath(
        os.path.expandvars(
            os.path.expanduser(a.proxy)))

    configpath = os.path.abspath(
        os.path.expandvars(
            os.path.expanduser(a.userconfig)))

    if os.path.isfile(configpath):
        config_parser = SafeConfigParser(interpolation=None)
        with open(configpath) as UC:
            config_parser.readfp(UC)

        if config_parser.has_section('cadc'):
            username = config_parser.get('cadc', 'cadc_id')
            passwd = config_parser.get('cadc', 'cadc_key')

    elif os.access(os.path.join(os.environ.get('HOME', '/'), ".netrc"),
                   os.R_OK):
        auth = netrc.netrc().authenticators(host)
        username = auth[0]
        passwd = auth[2]

    needsupdate = True
    if os.path.exists(cadcproxy):
        needsupdate = subprocess.call(['/usr/bin/openssl',
                                       'x509',
                                       '-in',
                                       cadcproxy,
                                       '-noout',
                                       '-checkend',
                                       secvalid])
    if needsupdate:
        renew(cadcproxy, username, passwd, a.daysvalid)
    else:
        logger.debug('Certificate is still valid')
