#!/usr/bin/env python2.7

__author__ = "Russell O. Redman"


import errno
from contextlib import contextmanager
import datetime
import logging
import os.path
import re
import subprocess
from threading import Event
from threading import Lock
import traceback

try:
    import Sybase
    sybase_defined = True
except ImportError:
    sybase_defined = False

from tools4caom2 import __version__

__doc__ = """
The database class immplements thread-safe methods methods to interact with 
Sybase databases. 

Version: """ + __version__.version

class database(object):
    """
    Manage connection to Sybase.
    Credentials are read from userconfig, which is a SafeConfigParser object.
    
    The userconfig dictionary can contain definitions for:
    'server': Sybase server
    
    'cred_id': database user account
    'cred_key': database user password
    
    'read_db': database to read by default ('cred_db' if absent)
    'write_db': database to write by default ('cred_db' if absent)
    
    It is legitimate to omit all of these if no connection is needed.
    
    Usage:
    userconfig['server'] = 'SYBASE'
    mylog = tools4caom2.logger("mylogfile.log")
    
    with database(userconfig, mylog) as db:
        cmd = 'SELECT max(utdate) from ' + jcmt_db + 'COMMON'
        max_utdate = db.read(cmd)[0][0]
    
        upodate_cmd = '''UPDATE state = "W"
                         FROM jcmt_discovery
                         WHERE discovery_id = ''' % (IDVALUE,)
        with db.transaction():
            db.write(update_cmd)
    
    This class creates separate singleton connections for read and write
    operations, protected with a mutex so the code should be thread-safe.
    """
    
    # class attributes read_mutex and write_mutex
    read_mutex = Lock()
    write_mutex = Lock()
    
    # class attributes read_connection and write_connection
    read_connection = None
    write_connection = None
        
    # class constants for missing data of different types
    NULL = {'query': {'string': '"NULL"',
                      'integer': -9999,
                      'float': -9999.0,
                      'datetime': '"2199-01-01 00:00:00.0"'},
            'value': {'string': 'NULL',
                      'integer': -9999,
                      'float': -9999.0,
                      'datetime': datetime.datetime(2199, 01, 01, 0, 0, 0)}}

    class ConnectionError(Exception):
        """
        Report a connection error.
        """
        def __init__(self, value):
            self.value = value

    def __init__(self, userconfig, log, use=True):
        """
        Create a new connection to the Sybase server
        
        Arguments:
        userconfig: SafeConfigParser object
        log: the instance of tools4caom2.logger.logger to use
        
        Exceptions:
        IOError: if call to dbrc_get fails
        IOError: if dbrc_get cannot read credentials for SYBASE jcmtmd.dbo
        IOError: if connection to SYBASE fails

        It is legitimate to customize the pause_queue for each connection.
        """
        if sybase_defined:
            self.use = use
        else:
            self.use = False
        self.log = log

        # Database server to use for queries
        self.server = None
        
        # Database account and password
        self.cred_id = None
        self.cred_key = None
        
        # Database to use for default read and write access
        self.read_db = None
        self.write_db = None

        # Always read the userconfig to configure database connection
        if userconfig.has_section('database'):
            if userconfig.has_option('database', 'server'):
                self.server = userconfig.get('database', 'server')
            
            if userconfig.has_option('database', 'read_db'):
                self.read_db = userconfig.get('database', 'read_db')
        
            if userconfig.has_option('database', 'write_db'):
                self.write_db = userconfig.get('database', 'write_db')
            elif userconfig.has_option('database', 'read_db'):
                self.write_db = userconfig.get('database', 'read_db')

            if userconfig.has_option('database', 'cred_id'):
                self.cred_id = userconfig.get('database', 'cred_id')
            if userconfig.has_option('database', 'cred_key'):
                self.cred_key = userconfig.get('database', 'cred_key')
        else:
            self.use = False

        self.log.file('sybase_defined: ' + str(sybase_defined), logging.DEBUG)
        self.log.file('database use: ' + str(self.use), logging.DEBUG)
        if self.server:
            self.log.file('database server: ' + self.server, logging.DEBUG)
        if self.read_db:
            self.log.file('database read_db: ' + self.read_db, logging.DEBUG)
        if self.write_db:
            self.log.file('database write_db: ' + self.write_db, logging.DEBUG)
        self.pause_queue = [1.0, 2.0, 3.0]
        # if self.cred_id:
        #     self.log.file('database cred_id: ' + self.cred_id, logging.DEBUG)
        # if self.cred_key:
        #     self.log.file('database cred_key: ' + self.cred_key, logging.DEBUG)

    def available(self):
        """
        Return True if it is possible to query the database, False otherwise 
        """
        return self.use
    
    def get_credentials(self):
        """
        For use at the CADC only.  
        Read the users .dbrc file to get credentials, if dbrc_get is defined
        and userconfig does not define cred_id and cred_key
        
        Arguments:
        <None>
        """
        if (sybase_defined and self.use
            and not (self.cadc_id and self.cadc_key)):
            
            try:
                output = subprocess.check_output(['which', 'dbrc_get'],
                                                 stderr=subprocess.STDOUT)
                use_config = True
            except:
                use_config = False
            if use_config:
                try:
                    credcmd = ['dbrc_get', self.server,  self.cred_db]
                    credentials = subprocess.check_output(credcmd,
                                                    stderr=subprocess.STDOUT)

                    cred = re.split(r'\s+', credentials)
                    if len(cred) < 2:
                        self.log.console('cred = ' + repr(cred) +
                                         ' should contain username, password',
                                         logging.ERROR)

                    self.cadc_id = cred[0]
                    self.cadc_key = cred[1]
                except subprocess.CalledProcessError as e:
                    self.log.console('errno.' + errno.errorcode(e.returnvalue) +
                                     ': ' + credentials,
                                     logging.ERROR)
        
    def get_read_connection(self):
        """
        Create a singleton read connection if necessary.
        Only called from inside the read() method, this is protected by
        the database.read_mutex of that method.  It is an error to call
        read if the database is not available.
        
        Arguments:
        <None>
        """
        if sybase_defined and self.use:
            if not database.read_connection:
                # self.get_credentials()
                self.log.file('have credentials')
                # Check that credentials exist
                if not (self.cred_id and self.cred_key):
                    
                    self.log.file('No user credentials, so omit '
                                  'opening connection to database')
                else:
                    database.read_connection = \
                        Sybase.connect(self.server,
                                       self.cred_id,
                                       self.cred_key,
                                       database=self.read_db,
                                       auto_commit=1,
                                       datetime='python')
                    if not database.read_connection:
                        self.log.console('Could not connect to ' + 
                                         self.server + ':' +
                                         self.read_db,
                                         logging.ERROR)
        else:
            self.log.file('cannot open a read_connection to a database '
                          'because Sybase is not available',
                          logging.ERROR)

            
    def get_write_connection(self, write_db):
        """
        Create a singleton write connection if necessary
        Only called from inside the write() method, this is protected by
        the database.write_mutex of that method.  It is an error to call
        write if the database is not available.
        
        Arguments:
        <None>
        """
        if sybase_defined and self.use:
            if not database.write_connection:
                # self.get_credentials()
                # Check that credentials exist
                if not (self.cred_id and self.cred_key):
                    
                    self.log.file('No user credentials, so omit '
                                  'opening connection to database')
                else:
                    database.write_connection = \
                        Sybase.connect(self.server,
                                       self.cred_id,
                                       self.cred_key,
                                       database=self.write_db,
                                       auto_commit=0,
                                       datetime='python')
                    if not database.write_connection:
                        self.log.console('Could not connect to ' + 
                                         self.server + ':' +
                                         self.write_db,
                                         logging.ERROR)
        else:
            self.log.file('Could not open a write_connection to a database '
                           'because Sybase is not available',
                           logging.ERROR)

    def read(self, query, params={}):
        """
        Run an sql query, multiple times if necessary, using read_connection.
        Only one read can be active at a time, protected by the read_mutex,
        but it can run in parallel with a write transaction.

        Arguments:
        query: a properly formated SQL select query
        params: dictionary of parameters to pass to execute
        """
        returnList = []
        self.log.file(query)
        retry = True
        number = 0
        while sybase_defined and retry:
            try:
                self.log.file('acquiring read_mutex...', logging.DEBUG)
                with database.read_mutex:
                    self.log.file(
                        'read_mutex acquired, obtaining connection...',
                        logging.DEBUG)
                    self.get_read_connection()
                    self.log.file(
                        'read_connection obtained, obtaining cursor...',
                        logging.DEBUG)
                    try:
                        cursor = database.read_connection.cursor()
                        self.log.file('cursor obtained, exceuting query...',
                                      logging.DEBUG)
                        cursor.execute(query, params)
                        self.log.file('query executed, fetching results...',
                                      logging.DEBUG)
                        returnList = cursor.fetchall()
                        self.log.file('results fetched', logging.DEBUG)
                    finally:
                        self.log.file('closing cursor...', logging.DEBUG)
                        cursor.close()
                        self.log.file('cursor closed', logging.DEBUG)
                    retry = False
                    # should be a no-op
                    # database.read_connection.commit()
            except Exception as e:
                # Do not know what kind of error we will get back
                # only the last one will be reported
                if number < len(self.pause_queue):
                    self.log.console('cursor returned error: '
                                     'wait for %.1f seconds and retry' %
                                     (self.pause_queue[number],),
                                     logging.WARN)
                    t = Event()
                    t.wait(self.pause_queue[number])
                    number += 1
                else:
                    retry = False
                    self.log.console(traceback.format_exc())
                    raise
        return returnList

    def write(self, cmd, params={}, result=False):
        """
        Run an sql query, multiple times if necessary, using write_connection.
        Only one write can be active at a time, protected by the write_mutex.
        Write operations should be done inside a transaction.

        Arguments:
        cmd: a properly formated SQL insert or select into command
        params: dictionary of parameters to pass to execute
        
        The query should not return any rows of output.
        """
        self.log.file(cmd)
        returnList = []
        number = 0
        retry = True
        while sybase_defined and retry:
            try:
                with database.write_mutex:
                    self.get_write_connection()
                    try:
                        cursor = database.write_connection.cursor()
                        cursor.execute(cmd, params)
                        returnList = cursor.fetchall()
                    finally:
                        cursor.close()
                    retry = False
            except Exception as e:
                # Do not know what kind of error we will get back
                # only the last one will be reported
                if number < len(self.pause_queue):
                    self.log.console('cursor returned error: '
                                     'wait for %.1f seconds and retry' %
                                     (self.pause_queue[number],),
                                     logging.WARN)
                    t = Event()
                    t.wait(self.pause_queue[number])
                    number += 1
                else:
                    retry = False
                    self.log.console(traceback.format_exc())
                    raise
        return returnList
    
    @contextmanager
    def transaction(self):
        """
        Start a database transaction using the write connection.  Only one
        transaction can be executed at a time, protected using the
        write_mutex, but it can comprise several select, insert and select into
        statements.
        
        Raising any exception other than a ConnectionError will cause the 
        transaction to be rolled back.  A ConnectionError indicates that the
        connection has been dropped.  The transaction should have been
        rolled back automatically.
        
        Otherwise, the transaction will be commited.  
        """
        if sybase_defined:
            try:
                self.write('BEGIN TRANSACTION')
                yield
            except database.ConnectionError as e:
                self.log.console('write_connection has failed BEGIN TRANSACTION:'
                                 + str(e),
                                 logging.ERROR)
            except Exception as e:
                self.write('ROLLBACK')
                self.log.console('The write_connection has been rolled back:'
                                 + str(e),
                                 logging.ERROR)
            else:
                self.write('COMMIT')
            
    @classmethod
    def close(cls):
        """
        Close the database conenction
        
        Arguments:
        cls        the class that called the method (ignored)
        """
        if sybase_defined:
            if database.read_connection:
                database.read_connection.close()
                database.read_connection = None

            if database.write_connection:
                database.write_connection.close()
                database.write_connection = None

@contextmanager
def connection(userconfig, log, use=True):
    """
    Context manager that creates and yields a database object that
    can be used to create read and write connections, then closes the 
    connections automatically on completion.
    
    Arguments:
    server: one of "DEVSYBASE" or "SYBASE"
    database: database to use for credentials
    log: the instance of tools4caom2.logger.logger to use
    """
    try:
        yield database(userconfig, log, use=use)
    finally:
        database.close()
