#!/usr/bin/env python

__author__ = "Russell O. Redman"

import argparse
from ConfigParser import SafeConfigParser
import netrc
import os.path
import subprocess
import sys
import urllib2

def renew(proxypath, username, passwd, daysvalid):
    """
    Renew the proxy certificate
    
    Arguments:
    proxypath: path to the proxy certificate (pem) file
    username:  username to use for 
    """
    certHost='http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca'
    certQuery="/cred/proxyCert?daysValid="

    ## Example taken from voidspace.org.uk
    # create a password manager
    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()

    # Add the username and password.
    # If we knew the realm, we could use it instead of ``None``.
    password_mgr.add_password(None, certHost, username, passwd)
    
    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
    
    # create "opener" (OpenerDirector instance)
    opener = urllib2.build_opener(handler)
    
    # Install the opener.   
    urllib2.install_opener(opener)

    # Now all calls to urllib2.urlopen use our opener.
    url = ''.join([certHost, certQuery, str(daysvalid)])
    r = urllib2.urlopen(url)
    with open(proxypath,'w') as w:
        while True:
            buf = r.read()
            if not buf:
                break
            w.write(buf)
    r.close()
    return 

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
    a = ap.parse_args()

    minvalid = min(a.minvalid, a.daysvalid)
    secvalid = str(86400*minvalid)
    
    cadcproxy = os.path.abspath(
                    os.path.expandvars(
                        os.path.expanduser(a.proxy)))
    
    configpath = os.path.abspath(
                    os.path.expandvars(
                        os.path.expanduser(a.userconfig)))
    
    if os.path.isfile(configpath):
        config_parser = SafeConfigParser()
        with open(configpath) as UC:
            config_parser.readfp(UC)
    
        if config_parser.has_section('cadc'):
            username = config_parser.get('cadc', 'cadc_id')
            passwd = config_parser.get('cadc', 'cadc_key')
    
    elif os.access(os.path.join(os.environ.get('HOME','/'),".netrc"), os.R_OK):
        auth = netrc.netrc().authenticators(host)
        username = auth[0]
        passwd = auth[2]

    needsupdate = subprocess.call(['openssl', 
                                   'x509',
                                   '-in',
                                   cadcproxy,
                                   '-noout',
                                   '-checkend',
                                   secvalid])
    if needsupdate:
        renew(cadcproxy, username, passwd, a.daysvalid)
