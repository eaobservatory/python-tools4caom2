import argparse
from astropy.io.votable import parse
import astropy.io.ascii
import httplib
import logging
import os
import os.path
import re
import requests
import StringIO
import sys
import traceback

from tools4caom2.error import CAOMError
from tools4caom2.utdate_string import utdate_string
from tools4caom2.util import configure_logger

from tools4caom2.__version__ import version as tools4caom2version

logger = logging.getLogger(__name__)


class tapclient(object):
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

    CADC_TAP_SERVICE = 'https://www1.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap/sync'

    def __init__(self,
                 proxy='$HOME/.ssl/cadcproxy.pem'):
        """
        TAP queries using ADQL return a VOtable or an astropy Table

        Arguments:
        proxy: (optional) path to a proxy certificate
        """
        self.cadcproxy = os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(proxy)))

    def query(self, adql, format='table'):
        """
        Send an adql query to the service and store the response in a file-like
        object.

        Arguments:
        adql: a text string containing and ADQL query
        format: text string indicating whether the desired output format
                should be an astropy.table.Table (default) or the raw votable
        """
        logger.debug('ADQL: %s', adql)
        query = re.sub(r'\s+', ' ', adql.strip())
        params = {'REQUEST': 'doQuery',
                  'LANG': 'ADQL',
                  'QUERY': query}

        table = None

        try:
            r = requests.get(tapclient.CADC_TAP_SERVICE,
                             params=params,
                             cert=self.cadcproxy)
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
                if format == 'table':
                    try:
                        table = vot.get_first_table().to_table()
                    except:
                        table = None

            elif r.status_code != 404:
                logger.warning('%s = %s: %s', r.status_code,
                               httplib.responses[r.status_code],
                               r.content)
        except Exception as e:
            logger.warning('FAILED to get reply for "%s": %s', adql,
                           traceback.format_exc())

        return table


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
    ap.add_argument('-v', '--verbose',
                    action='store_true',
                    help='output extra information')
    ap.add_argument('values',
                    nargs='*',
                    help='values to be substituted in the format codes')
    a = ap.parse_args()

    configure_logger()

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

    if a.votable:
        votable = tap.query(adqlquery, 'votable')
        if votable:
            astropy.io.votable.table.writeto(votable, a.out)
    else:
        table = tap.query(adqlquery)
        if a.out:
            OUTFILE = open(a.out, 'w')
        else:
            OUTFILE = sys.stdout
        try:
            if table:
                astropy.io.ascii.write(table,
                                       OUTFILE,
                                       Writer=astropy.io.ascii.FixedWidth)
        finally:
            if a.out:
                OUTFILE.close()