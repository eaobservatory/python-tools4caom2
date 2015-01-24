#!/usr/bin/env python2.7

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

from tools4caom2.logger import logger
from tools4caom2.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version


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
                 log,
                 proxy='$HOME/.ssl/cadcproxy.pem'):
        """
        TAP queries using ADQL return a VOtable or an astropy Table

        Arguments:
        log: an instance of a tools4caom2.logger
        proxy: (optional) path to a proxy certificate
        """
        self.log = log
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
        self.log.file(adql)
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
                    self.log.console('TAP QUERY response: ' + query_content,
                                     logging.ERROR)
                elif query_status != 'OK':
                    if query_content:
                        self.log.console('TAP QUERY_STATUS = ' +
                                         str(query_status) +
                                         '  MESSAGE = ' + query_content,
                                         logging.ERROR)

                # Get here if the table is valid, so process the contents...
                # copy dictionary for usage after r is closed
                if format == 'table':
                    try:
                        table = vot.get_first_table().to_table()
                    except:
                        table = None

            elif r.status_code != 404:
                self.log.console(str(r.status_code) + ' = ' +
                                 httplib.responses[r.status_code] +
                                 ': ' + str(r.content),
                                 logging.WARN)
        except Exception as e:
            self.log.console('FAILED to get reply for "' + adql + '": ' +
                             traceback.format_exc(),
                             logging.WARN)
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

    log = logger('tapclient_' + utdate_string() + '.log')

    tap = tapclient(log)

    if os.path.isfile(a.adql):
        with open(a.adql, 'r') as ADQL:
            adqlquery = ADQL.read()
    else:
        adqlquery = a.adql

    if a.values:
        adqlquery = adqlquery % tuple(a.values)
    if a.verbose:
        log.console(adqlquery)

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
