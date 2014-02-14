#!/usr/bin/env python2.7
"""
Client for testlogger module
"""
import argparse
import logging

from tools4caom2.logger import logger

def log_messages():
    """Log some messages"""
    
    ap = argparse.ArgumentParser()
    ap.add_argument('--log')
    ap.add_argument('--loglevel',
                    choices=['debug', 'info', 'warn'],
                    default='info')
    ap.add_argument('--console_output',
                    choices=['True', 'False'],
                    default='True')
    args = ap.parse_args()
    
    if args.log:
        logfilename = args.log
    else:
        logfilename = 'clientLogger.log'
    
    loglevel = logging.INFO
    if args.loglevel == 'debug':
        loglevel = logging.DEBUG
    elif args.loglevel == 'warn':
        loglevel = logging.WARN    
    
    console_output = args.console_output == 'True'
    
    with logger(logfilename, 
                loglevel=loglevel,
                console_output=console_output).record() as log:
        log.console('MESSAGE1')
        log.console('DEBUG1', logging.DEBUG)
        log.console('INFO1', logging.INFO)
        log.console('WARNING1', logging.WARN)
        
if __name__ == '__main__':
    log_messages()
    
    
    