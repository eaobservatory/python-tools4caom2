# Copyright (C) 2014-2015 Science and Technology Facilities Council.
# Copyright (C) 2015 East Asian Observatory.
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


import argparse
from astropy.io.votable import parse
import astropy.io.ascii
import httplib
import logging
import os
import os.path
import requests
import StringIO
import sys
import traceback

from tools4caom2.error import CAOMError
from tools4caom2.util import configure_logger

from tools4caom2.__version__ import version as tools4caom2version

logger = logging.getLogger(__name__)


class tapclient_cadc(object):
    """
    Query the CADC TAP (Table Access Protocol) service using ADQL to
    request data from the database.

    Expected usage might be like:

    tap = tapclient()
    adql = ("SELECT count(*) AS count"
            "FROM caom2.Observation AS Observation "
            "WHERE O.collection = 'JCMT'")
    table = tap.query(adql)
    count = table[0]['count']
    """

    def __init__(self, service_url,
                 proxy='$HOME/.ssl/cadcproxy.pem'):
        """
        TAP queries using ADQL return a VOtable or an astropy Table

        Arguments:
        proxy: (optional) path to a proxy certificate
        """

        self.service_url = service_url

        self.cadcproxy = os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(proxy)))

    def query(self, adql, timeout=60):
        """
        Send an adql query to the service and store the response in a file-like
        object.

        Arguments:
        adql: a text string containing and ADQL query
        timeout: timeout (for start of response) in seconds [default 60]
        """
        logger.debug('ADQL: %s', adql)
        query = adql.strip()
        params = {'REQUEST': 'doQuery',
                  'LANG': 'ADQL',
                  'QUERY': query}

        try:
            r = requests.get(self.service_url,
                             params=params,
                             cert=self.cadcproxy,
                             timeout=timeout)
            if r.status_code == 200:
                # The TAP service handled the query and returned a VOTable,
                # but may not have run the query successfully.  Check for
                # error messages.
                vot = parse(StringIO.StringIO(r.content))
                query_status = None
                query_content = None
                if vot.resources and vot.resources[0].type == 'results':
                    for info in vot.resources[0].infos:
                        if info.name == 'QUERY_STATUS':
                            query_status = info.value
                            query_content = info.content
                if query_status == 'ERROR':
                    logger.error('TAP QUERY response: %s', query_content)
                    raise CAOMError('Tap query failed with an error')

                elif query_status != 'OK':
                    if query_content:
                        logger.error('TAP QUERY_STATUS = %s  MESSAGE = %s',
                                     query_status, query_content)
                        raise CAOMError('Tap query status not OK')

                # Get here if the table is valid, so process the contents...
                # copy dictionary for usage after r is closed
                return vot.get_first_table().to_table()

            else:
                logger.error('%s = %s: %s', r.status_code,
                             httplib.responses[r.status_code],
                             r.content)
                raise CAOMError('TAP query received HTTP response: {0}'.format(
                    httplib.responses[r.status_code]))

        except CAOMError:
            # Pass on any errors which were raised explicitly.
            raise

        except requests.exceptions.Timeout:
            raise CAOMError('TAP query timed out')

        except Exception as e:
            # Raise CAOMError for any other exception.
            logger.exception('FAILED to get reply for "%s"', adql)
            raise CAOMError('Error occurred  during TAP query')


class tapclient(tapclient_cadc):
    def __init__(self, *args, **kwargs):
        super(tapclient, self).__init__(
            'https://www1.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap/sync',
            *args, **kwargs)


class tapclient_ad(tapclient_cadc):
    def __init__(self, *args, **kwargs):
        super(tapclient_ad, self).__init__(
            'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/ad/sync',
            *args, **kwargs)


def run():
    """
    do a simple test
    """
    ap = argparse.ArgumentParser()
    ap.add_argument('--adql',
                    required=True,
                    help='string or text file containing an ADQL string that '
                         'can contain format codes')
    ap.add_argument('--out',
                    help='file to contain query results')
    ap.add_argument('--votable',
                    action='store_true',
                    help='output the response as a VOtable, else as text')
    ap.add_argument('--ad',
                    action='store_true',
                    help='use CADC AD TAP service')
    ap.add_argument('--timeout',
                    type=int, default=60,
                    help='timeout (seconds, default: 60)')
    ap.add_argument('-v', '--verbose',
                    action='store_true',
                    help='output extra information')
    ap.add_argument('values',
                    nargs='*',
                    help='values to be substituted in the format codes')
    a = ap.parse_args()

    configure_logger()

    if a.ad:
        tap = tapclient_ad()
    else:
        tap = tapclient()

    if os.path.isfile(a.adql):
        with open(a.adql, 'r') as ADQL:
            adqlquery = ADQL.read()
    else:
        adqlquery = a.adql

    if a.values:
        adqlquery = adqlquery % tuple(a.values)
    if a.verbose:
        logger.info(adqlquery)

    table = tap.query(adqlquery, timeout=a.timeout)

    if a.out:
        OUTFILE = open(a.out, 'w')
    else:
        OUTFILE = sys.stdout
    try:
        if a.votable:
            astropy.io.votable.table.writeto(table, a.out)
        else:
            if len(table):
                astropy.io.ascii.write(table,
                                       OUTFILE,
                                       Writer=astropy.io.ascii.FixedWidth)
    finally:
        if a.out:
            OUTFILE.close()
