#!/usr/bin/env python2.7
__author__ = "Russell O. Redman"

"""
A simple config file manager, where the config file is maintained as a text
file that can be encoded and decoded to obfuscate the contents. 
"""

import base64
import getpass
import logging
import os
import os.path
import re

class config(object):
    """
    Manage user configuration.  This class creates a user configuration file
    at a specified path, sets permissions appropriately to keep it private 
    to the individual user, encyphers the file to obfuscate the contents,
    and reads the file.
    
    The config file is a set of key = value pairs.  
    Comments start with the # character.
    
    Each file is associated with an object.  If a key matches the name of an 
    attribute in the managed object, the representation of the attribute value
    is stored as the value in the configuration file.  When the file is
    read, the stored representation is evaluated and the attribute is set
    to that value again. 
    
    Keys can be defined that do not match attributes in the managed object.
    These are stored in a dictionary in the config object and can be access
    using a dictionary-like mechanism.
    
    A config object emulates a dictionary by delegating the dictionary
    methods to the userconfig attribute.
    """
    firstline = '# config - do not change this line\n'

    def __init__(self, filepath):
        """
        Initialize the config structure but do not read or write the file, 
        which will be specified dynamically in a later step.
        
        Arguments:
        default: a dictionary giving the default set of keys to maintain
        manage: an object whose attributes will be managed by the user 
                configuration
        """
        self.userconfig = {}
        self.filepath = os.path.expanduser(os.path.expandvars(filepath))

        
    def create_if_not_present(self):
        """
        If the user configuration file does not exist, create it and set
        appropriate permissions on all new directories in the path.
        
        Arguments:
        filepath: the path to the user configuration file
        """
        dirname, filename = os.path.split(self.filepath)
        
        # If the path does not exist, create any missing directories
        # Permission will be for user-only access
        if not os.path.exists(dirname):
            os.makedirs(dirname, 0700)

        # If the file does not exist, create it and fill it with default values
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w') as F:
                F.write(config.firstline)
                for key in self.userconfig:
                    if self.userconfig[key] is not None:
                        F.write('%s = %s\n' % (key, 
                                               repr(self.userconfig[key])))
                    else:
                        F.write('# %s = %s\n' % (key, 
                                                 repr(self.userconfig[key])))
        self.toggle_view('encode')
        os.chmod(self.filepath, 0600)

    def toggle_view(self, state):
        """
        base64-encode the contents of the file
        
        Arguments:
        state: if state=="encode' then ensure the file is encoded
               if state=='decode' then ensure the file is decoded
        """
        with open(self.filepath, 'r') as F:
            s = F.read()

        with open(self.filepath, 'w') as F:
            if re.match(r'^' + config.firstline + '.*', s):
                if state != 'decode':
                    F.write(base64.b64encode(s))
            else:
                if state != 'encode':
                    F.write(base64.b64decode(s))
        os.chmod(self.filepath, 0600)

    def read(self):
        """
        Reads the file and returns a dictionary containing the key/value pairs
        
        Arguments:
        <none>
        """
        s = ''
        with open(self.filepath, 'r') as F:
            s = F.read()
            if re.match(r'^' + config.firstline + '.*', s):
                raise RuntimeError('For security the config file ' +
                                   self.path + ' must be encyphered',
                                   logging.ERROR)
            c = base64.b64decode(s)
        
        mydict = {}
        if c:
            for line in re.split(r'\n', c):
                # Strip off comments
                line = re.sub(r'#.*$', '', line)
                line.strip()
                
                if line:
                    # if anything remains and matches r'^key = value .*'
                    parts = re.split(r'\s*=\s*', line)
                    if len(parts) >= 2:
                        key = parts[0].strip()
                        value = parts[1].strip()
                        mydict[key] = eval(value)
        
            self.userconfig.update(mydict)
    
    # The dictionary is implemented by delegating the operations to
    # the userconfig attribute.
    def __len__(self):
        """
        Delegate len() to userconfig 
        """
        return len(self.userconfig)
        
    def __getitem__(self, key):
        """
        Delegate getitem to userconfig 
        """
        return self.userconfig[key]
        
    def get(self, key, default=None):
        """
        Delegate getitem to userconfig 
        """
        if default:
            return self.userconfig.get(key, default)
        else:
            return self.userconfig.get(key)
        
    def __setitem__(self, key, value):
        """
        Delegate getitem to userconfig 
        """
        self.userconfig[key] = value

    def __delitem__(self, key):
        """
        Delegate getitem to userconfig 
        """
        del self.userconfig[key]

    def __iter__(self):
        """
        Delegate __iter__ to userconfig 
        """
        return self.userconfig.__iter__()

    def iterkeys(self):
        """
        Delegate __iter__ to userconfig 
        """
        return self.userconfig.iterkeys()

    def keys(self):
        """
        Delegate __iter__ to userconfig 
        """
        return self.userconfig.keys()

    def __reversed__(self):
        """
        Delegate __iter__ to userconfig 
        """
        return self.userconfig.__reversed__()

    def __contains__(self, key):
        """
        Delegate getitem to userconfig 
        """
        return key in self.userconfig
