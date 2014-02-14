#!/usr/bin/env python
#/*+
#************************************************************************
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#*
#* (c) 2013  .                      (c) 2013
#* National Research Council        Conseil national de recherches
#* Ottawa, Canada, K1A 0R6          Ottawa, Canada, K1A 0R6
#* All rights reserved              Tous droits reserves
#*
#* NRC disclaims any warranties,    Le CNRC denie toute garantie
#* expressed, implied, or statu-    enoncee, implicite ou legale,
#* tory, of any kind with respect   de quelque nature que se soit,
#* to the software, including       concernant le logiciel, y com-
#* without limitation any war-      pris sans restriction toute
#* ranty of merchantability or      garantie de valeur marchande
#* fitness for a particular pur-    ou de pertinence pour un usage
#* pose.  NRC shall not be liable   particulier.  Le CNRC ne
#* in any event for any damages,    pourra en aucun cas etre tenu
#* whether direct or indirect,      responsable de tout dommage,
#* special or general, consequen-   direct ou indirect, particul-
#* tial or incidental, arising      ier ou general, accessoire ou
#* from the use of the software.    fortuit, resultant de l'utili-
#*                                  sation du logiciel.
#*
#************************************************************************
#*
#*   Script Name:    logger.py
#*
#*   Purpose:
#+    Module defining the class logger, a utility that can log to the console,
#+        to a file, and to an e-mail message, or any combination, and
#+        interacts well with java loggers.
#*
#*   Classes:
#+    logger
#*
#*   Functions:
#+    __init__(filename[,loglevel],[sender,to,smtphost,subject])
#+    logger.record() (context manager for use in a "with" statement
#+    logger.console(message[,logging.LEVEL])
#+    logger.file(message[,logging.level])
#+    logger.send_email()
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
__author__ = "Russell O. Redman"

from contextlib import contextmanager
from email.mime.text import MIMEText
import exceptions
import inspect
import logging, logging.config
import re
import smtplib
import string
import sys
import traceback

from tools4caom2 import __version__

__doc__ = """
logger module, a utility class that can log to the console, to a file, and
to an e-mail message.

Version: """ + __version__.version


class logger(object):
    '''A logging class that plays well with java loggers (to the disk file
    at least).  A log file must be specified at initialization.  Log messages
    can be sent directly to the log file or to both the console and the log
    file.  If e-mail configuration is specified on initialization, the console
    log messages are separately recorded and can be sent after the log is
    complete.

    To simplify the usage, the record method is a contextmanager intended to be
    used in a "with" statement like:
        with logger("mylogfile.log",
                   sender = 'My.Namer@nrc-cnrc.gc.ca',
                   to=['My.Name@nrc-cnrc.gc.ca', 'Your.Name@nrc-cnrc.gc.ca'],
                   subject='standard log file for this job'
                   ).record() as log:
           # log some messages
           bad = True
           log.console('Progress: starting')
           log.file('Inspect the value of bad: ' + str(bad), logging.WARNING)
           if bad:
               # Execution will stop here because this raises an exception
               log.console('Bad error - exit immediately' , logging.ERROR)

    When execution leaves the with statement for any reason, including an
    exception, the log file will be considered complete.  If e-mail contact
    information has been configured, an e-mail message will be sent.

    Logging a message with loglevel >= logging.ERROR will raise a 
    logger.LoggerError, an internal exception type derived from RuntimeError.  
    This will normally terminate the program.  If it should continue running, 
    such messages should be logged in a try-except block to catch the 
    exception:
        try:
            log.console('Serious Trouble', logging.ERROR)
        except logger.LoggerError:
            log.console('... but continuing anyways')
            pass
    Note that error messages should normally be logged to the console so that
    they are immediately visible and are included in the e-mail message.
    
    The record method has an internal try-except block that catches and logs 
    to the console any unexpected exceptions before re-raising the exception.  

    The "console" is normally sys.stderr.  The code checks whether stderr is
    a tty and bypasses the write to stderr if it is not.  This ensures that
    console messages still make it into the log file and e-mail message, even 
    when running under a non-interactive shell (e.g. gridengine).

    If e-mail configuration (sender, to, smtphost and subject) are given, two
    mechanisms are provided to send e-mail messages to the list of recipients
    specified in the argument "to".
    log.send_message(subject, message)
        - send the message with the given subject, logging the message subject
    log.send_email()
        - send the accumulated text logged to the console,
          then reset the accumulator
    If the record() method is used, the final act is to call send_email(),
    sending the accumulated text since the start of the logger or the last call
    to send_email.
    '''
    class LoggerError(exceptions.RuntimeError):
        """
        LoggerError is a flavour of RuntimeError
        """
        def __init__(self, message):
            exceptions.RuntimeError.__init__(self, message)

    def __init__(self,
                 filename,
                 loglevel=logging.INFO,
                 sender=None,
                 to=[],
                 smtphost='smtp.hia.nrc.ca',
                 subject=None,
                 console_output=True):
        """
        Initialize an instance of the logger class.

        Arguments:
        filename       - mandatory name of a log file
        loglevel       - optional show messages with log level >= loglevel
        sender         - optional e-mail address of the sender
        to             - optional list of e-mail recient addresses
        smtphost       - optional host to handle outgoing smtp messages
        subject        - optional subject for the e-mail message
        console_output - optional, console() will NOT write to console if False

        If any of sender, to, or subject is included, all three should be.
        """
        # Logging configuration dictionary
        LOGGING_CONFIG = {
            'version': 1,
            'formatters': {
                'consoleformat': {
                    'format': '%(levelname)s %(asctime)s %(message)s'
                },
                'fileformat': {
                    'format': '%(levelname)s %(asctime)s %(message)s',
                    'datefmt': '%Y-%m-%dT%H:%M:%S'
                }
            },
            'handlers': {
                'consolehandler': {
                    'class': 'logging.StreamHandler',
                    'stream': 'ext://sys.stderr',
                    'formatter': 'consoleformat'
                },
                'filehandler': {
                    'class': 'logging.FileHandler',
                    'filename': filename,
                    'formatter': 'fileformat'
                }
            },
            'loggers': {
                'console': {
                    'handlers': ['consolehandler'],
                    'level': loglevel,
                    'propagate': True
                }
            },
            'root': {
                'handlers': ['filehandler'],
                'level': logging.DEBUG
            }
        }

        # store the console text in a string that can be sent by e-mail
        self.text = None
        self.console_output = console_output
        self.loglevel = loglevel

        # if an e-mail message is desired,
        # specify all four of sender, to, smtphost and subject.
        self.sender = sender
        self.to = to
        self.smtphost = smtphost
        self.subject = subject

        # the request for an e-mail copy of the consol log is signalled by
        # self.text != None
        if (sender != None and (to != []) and
                    smtphost != None and subject != None):
            self.text = ''

        # see http://www.python.org/dev/peps/pep-0282/
        #logging.basicConfig(filename=filename,
        #                    format='%(levelname)s %(asctime)s %(message)s',
        #                    datefmt='%Y-%m-%dT%H:%M:%S',
        #                    level=loglevel)

        logging.config.dictConfig(LOGGING_CONFIG)

        self.additional_loggers = {}

        # consoleHandler writes messages to sys.stderr
        #self.consoleHandler = logging.StreamHandler()
        #self.consoleHandler.setLevel(self.loglevel)
        #self.formatter = logging.Formatter(
        #                           '%(levelname)s %(asctime)s %(message)s')
        #self.consoleHandler.setFormatter(self.formatter)

        # 'console' is a child of the root logger ''
        # messages passed to 'console' are also logged by ''
        self.additional_loggers['console'] = logging.getLogger('console')
        #self.additional_loggers['console'].propagate = True
        #self.additional_loggers['console'].addHandler(self.consoleHandler)

    def get_text(self):
        """
        Return the contents of the text buffer, or '' if the buffer is None.

        Arguments:
        <none>
        """
        if self.text == None:
            return ''
        else:
            return self.text

    def set_text(self, value=''):
        """
        Set the text buffer to a specified value, defaulting to ''

        Arguments:
        value - a value to which the text buffer will be set
        """
        self.text = value

    def add_logger(self, logsystem, console=False):
        """
        Add a logger to the system.

        Arguments:
        logsystem - the name of the system to log, e.g. 'mechanize.cookies'
        console   - send to console if True, to log file otherwise

        Note that nothing happens if the logsystem is already present.
        Beware that logging to the console can fail if the process does not
        have a console (e.g. running as a cron, condor or gridengine job).
        """
        if not logsystem in self.additional_loggers.keys():
            self.additional_loggers[logsystem] = logging.getLogger(logsystem)
            self.additional_loggers[logsystem].propagate = True
            self.additional_loggers[logsystem].setLevel(self.loglevel)
            if console:
                self.additional_loggers[logsystem].addHandler(
                                                           self.consoleHandler)

    def get_logger(self, logsystem):
        """
        Find and return a logger from additional_loggers

        Arguments:
        logsystem - the name of the system to log, e.g. 'mechanize.cookies'

        This can be used to customize the settings for a particular logger,
        e.g.set a loglevel for a particular logger that is
        different from the one used for the console and log file.
        """
        return self.additional_loggers[logsystem]

    def use_logger(self, logsystem, message, loglevel=logging.INFO):
        """
        Send a message to additional_loggers[logsystem] and to the log file.

        Arguments:
        logsystem - the name of the logger to use, e.g. 'mechanize.cookies'
        message  - mandatory string containing the log message
        loglevel - option logging level, default = logging.INFO

        Example:
        myLogger =  tools4caom2.logger(...)
        myLogger.add_logger('subsystem')
        myLogger.use_logger('subsystem', 'my message', loglevel=logging.INFO)
        """
        padded_message = message
        if loglevel >= logging.ERROR:
            cframe = inspect.currentframe()
            padded_message = '\n'.join(traceback.format_stack(cframe.f_back))
            padded_message += '\nERROR: '
            padded_message += message

        if self.text is not None:
            self.text += (padded_message + "\n")

        # send the message to the requested logger
        self.additional_loggers[logsystem].log(loglevel, padded_message)

        # raise the exception
        # no need to catch this error
        if loglevel >= logging.ERROR:
            raise logger.LoggerError(padded_message)

    def console(self, message, loglevel=logging.INFO):
        """
        Send a message to the console (stderr) and to the log file.

        Arguments:
        message  - mandatory string containing the log message
        loglevel - option logging level, default = logging.INFO

        Important messages (e.g. error messages) should be sent to the console.
        Setting loglevel == logging.ERROR will raise a logger.LoggerError.

        If stderr is not a tty, writing to that message stream will be skipped.

        If e-mail configuration has been supplied, messages to the console will
        be collected and sent as the body of the e-mail.  Note that this might
        use a fair amount of additional memory.
        """
        padded_message = message
        if loglevel >= logging.ERROR:
            cframe = inspect.currentframe()
            padded_message = '\n'.join(traceback.format_stack(cframe.f_back))
            padded_message += '\nERROR: '
            padded_message += message

        if self.text is not None:
            self.text += (padded_message + "\n")

        # Console messages are nominally logged to stderr.  If this is not an
        # interactive tty, bypass that logging step.
        if self.console_output:
            self.additional_loggers['console'].log(loglevel, padded_message)
        else:
            logging.log(loglevel, padded_message)

        # raise the exception
        # no need to catch this error
        if loglevel >= logging.ERROR:
            raise logger.LoggerError(padded_message)

    def file(self, message, loglevel=logging.INFO):
        """
        Send a message to the log file.

        Arguments:
        message  - mandatory string containing the log message
        loglevel - option logging level

        This message will NOT be sent to the console, and will NOT be included
        in the e-mail message.  This is a useful way to add detailed
        information to the log file that would disrupt the logical coherence
        of the console log stream.

        Setting loglevel == logging.ERROR will raise a logger.LoggerError.
        Note that important messages such as error messages should be sent to
        the console.
        """
        padded_message = message
        if loglevel >= logging.ERROR:
            cframe = inspect.currentframe()
            padded_message = '\n'.join(traceback.format_stack(cframe.f_back))
            padded_message += '\nERROR: '
            padded_message += message

        logging.log(loglevel, padded_message)
        if loglevel >= logging.ERROR:
            raise logger.LoggerError(padded_message)

    def send_email(self):
        """
        Send the current buffer of console messages by e-mail and reset the
        buffer to empty, if the e-mail configuration has been supplied.  
        Does nothing if e-mail has not been configured.

        Arguments:
        <none>

        This can be called several times, and each time will send the
        accumulated console text since the creation of the log or last call.
        """
        if self.text and self.subject:
            self.send_message(self.subject, self.text)
            self.text = ''

    def send_message(self, subject, text):
        """
        Send text as an e-mail message with the given subject, if the e-mail
        contact information has been configured. The subject is logged to the
        console, although the message itself is not.

        Arguments:
        subject  - the subject of the message
        text     - the text for the body of the message
        """
        if self.sender and self.to and self.smtphost:
            if self.text:
                msg = MIMEText(text + "\n")
                msg['Subject'] = subject
                msg['From'] = self.sender
                msg['To'] = ', '.join(self.to)

                s = smtplib.SMTP(self.smtphost)
                s.sendmail(self.sender, self.to, msg.as_string())
                s.quit()

    @contextmanager
    def record(self):
        """
        A context manager for use in a 'with' statement.

        Arguments:
        <None>

        This can be invoked with code in the form:
            with logger(...).record() as log:
                log.console(message)
                log.file(message)
        The final action is to send the accumulated console messages (not the
        log file on the disk) by e-mail, if e-mail contact information has 
        been configured.
        
        LoggerError messages will already have been logged to the file, but 
        unexpected exceptions will be caught by the try block and logged 
        before exitting.
        """
        try:
            yield self
        except:
            if sys.exc_info()[0] != logger.LoggerError:
                self.console(traceback.format_exc())
            raise
        finally:
            self.send_email()
