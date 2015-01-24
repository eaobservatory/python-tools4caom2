#!/usr/bin/env python2.7

import commands
import datetime
import logging
import os.path
import re
import stat
import string
import time

from tools4caom2.logger import logger


class gridengine(object):
    """
    A class to help submit jobs to gridengine.
    For use only at the CADC.
    """

    def __init__(self,
                 log,
                 queue='cadcproc',
                 options=None,
                 preamble=None,
                 test=False):
        """
        Initialize a gridengine submission object

        Arguments:
        log: an instance of  tools4caom2.logger for reporting errors
             that occur when setting up and submitting the job
        options: options to be passed to qsub
        preamble: a string, or iterable returning strings that provides a
                  preamble for the submitted job
        """
        self.log = log
        self.test = test

        self.options = ' -q ' + queue
        if options:
            if isinstance(options, str):
                self.options += (' ' + options)
            else:
                self.log.console('options must be a string:' + type(options),
                                 logging.ERROR)
        else:
            self.options += ' -cwd -j yes'

        self.preamble = []
        if preamble:
            if isinstance(preamble, str):
                self.preamble.append(preamble)
            else:
                for line in preamble:
                    if isinstance(line, str):
                        self.preamble.append(line)
                    else:
                        self.log.console('every line in preamble must be '
                                         'a string:' + type(line),
                                         logging.ERROR)
        else:
            self.preamble = ['#!/bin/csh',
                             'echo HOSTNAME = $HOSTNAME',
                             'echo HOSTTYPE = $HOSTTYPE',
                             'which java']

        self.backoff = [10.0, 20.0, 40.0]

    # ***********************************************************************
    # Submit a single job to gridengine
    # ***********************************************************************
    def submit(self,
               processing_commands,
               cshfile,
               logfile):
        """
        Submit a set of jobs to gridengine to ingest the raw data from the
        specified utdates, one day per job.

        Arguments:
        processing_commands: string or iterable over strings containing
        commands
        cshfile: path to the csh file, which will be overwritten if it exists
        logfile: path to the log file for the gridengine job
        """
        # write the csh script that will be submitted to gridengine
        self.log.file('CSHFILE: ' + cshfile)
        CSH = open(cshfile, 'w')
        for line in self.preamble:
            print >>CSH, line
        print >>CSH, 'echo SCRIPT = ' + cshfile
        print >>CSH, 'echo LOGFILE = ' + logfile

        if 'PYTHONPATH' in os.environ:
            print >>CSH, 'setenv PYTHONPATH ' + os.environ['PYTHONPATH']
        if 'CADC_ROOT' in os.environ:
            print >>CSH, 'setenv CADC_ROOT ' + os.environ["CADC_ROOT"]
        print >>CSH, 'setenv DEFAULT_CONFIG_DIR $CADC_ROOT/config'

        if isinstance(processing_commands, str):
            print >>CSH, processing_commands
        else:
            # assume this is an iterable
            for cmd in processing_commands:
                self.log.file('CSH: ' + cmd)
                if isinstance(cmd, str):
                    print >>CSH, 'echo %s\n%s' % (cmd, cmd)
                else:
                    self.log.console('command in processing_commands is not '
                                     'a string: ' + type(cmd))

        CSH.close()
        os.chmod(cshfile, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        # submit the script to gridengine
        qsub_cmd = 'qsub  ' + self.options
        qsub_cmd += ' -o ' + logfile
        qsub_cmd += (' ' + cshfile)

        self.log.file('PROGRESS: ' + qsub_cmd)
        retry = 0
        repeat = True
        while repeat:
            try:
                if not self.test:
                    status, output = commands.getstatusoutput(qsub_cmd)
                    repeat = False
                else:
                    status = 0
                    output = ''
                    repeat = False
            except:
                if retry == len(self.backoff):
                    repeat = False
                else:
                    self.log.console('qsub failed - pause for %2.0f seconds' %
                                     (self.backoff[retry],))
                    time.sleep(self.backoff[retry])
                    retry += 1

        if status:
            # still failing after all these retries, so give up
            self.log.console('Could not submit to gridengine:\n' +
                             output,
                             logging.ERROR)
