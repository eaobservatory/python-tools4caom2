#!/usr/bin/env python2.7
#/*+
#************************************************************************
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#*
#* (c) 2013.                  (c) 2013.
#* National Research Council        Conseil national de recherches
#* Ottawa, Canada, K1A 0R6         Ottawa, Canada, K1A 0R6
#* All rights reserved            Tous droits reserves
#*
#* NRC disclaims any warranties,    Le CNRC denie toute garantie
#* expressed, implied, or statu-    enoncee, implicite ou legale,
#* tory, of any kind with respect    de quelque nature que se soit,
#* to the software, including        concernant le logiciel, y com-
#* without limitation any war-        pris sans restriction toute
#* ranty of merchantability or        garantie de valeur marchande
#* fitness for a particular pur-    ou de pertinence pour un usage
#* pose.  NRC shall not be liable    particulier.  Le CNRC ne
#* in any event for any damages,    pourra en aucun cas etre tenu
#* whether direct or indirect,        responsable de tout dommage,
#* special or general, consequen-    direct ou indirect, particul-
#* tial or incidental, arising        ier ou general, accessoire ou
#* from the use of the software.    fortuit, resultant de l'utili-
#*                     sation du logiciel.
#*
#************************************************************************
#*
#*   Script Name:    gridengine
#*
#*   Purpose:
#*    Define a class to submit jobs to gridengine.
#*
#+ Usage: grid = gridengine(log)
#*        grid.submit('ls -l', myscript.csh', 'myscript.log')
#+
#+ Options:
#+  -h, --help            show this help message and exit
#*
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/

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
    A class to help submit jobs to gridengine
    """
    
    def __init__(self, log, queue='cadcproc', options=None, preamble=None):
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
        self.options = ' -q ' + queue
        if options:
            if isinstance(options, str):
                self.options += (' ' + options)
            else:
                self.log.console('options must be a string:' + type(options),
                             logging.ERROR)
        else:
            self.options += '-cwd -j yes'
        
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
        
    #************************************************************************
    # Submit a single job to gridengine
    #************************************************************************
    def submit(self,
               processing_commands,
               cshfile,
               logfile):
        """
        Submit a set of jobs to gridengine to ingest the raw data from the
        specified utdates, one day per job.

        Arguments:
        processing_commands: string or iterable over strings containing commands
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
                    print >>CSH,'echo %s\n%s' % (cmd, cmd)
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
                status, output = commands.getstatusoutput(qsub_cmd)
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
