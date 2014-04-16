#!/usr/bin/env python2.7
__author__ = "Russell O. Redman"

import os
import os.path
import stat
import tempfile
import unittest

from tools4caom2.config import config

class managedDictClass(object):
    def __init__(self):
        self.integer = 3
        self.float = 123.456
        self.string = 'my string'
        self.dictionary = {'a': 'b', 'c': 12}
        self.extra = 'string that should not change'
                           
class managedSlotsClass(object):
    def __init__(self):
        self.__slots__ = ('integer', 'float', 'string', 'dictionary', 'extra') 
        self.integer = 3
        self.float = 123.456
        self.string = 'my string'
        self.dictionary = {'a': 'b', 'c': 12}
        self.extra = 'string that should not change'
                           
class testConfig( unittest.TestCase):
    def setUp(self):
        """
        Create a config object to be used for testing
        """
        self.tmpdir = tempfile.mkdtemp()
        self.filepath = os.path.join(self.tmpdir, 'configtest')
        self.testconfig = config(self.filepath)
        
    def tearDown(self):
        """
        delete the temporary file and directory
        """
        os.remove(self.filepath)
        os.rmdir(self.tmpdir)
        
    def testFilePermissions( self):
        """
        Test that permissions are set correctly
        """
        myc = managedDictClass()
        self.testconfig['integer']    = myc.integer
        self.testconfig.create_if_not_present()

        self.assertTrue(os.path.exists(self.filepath),
                        'Failed to create ' + self.filepath)
        
        filestat = os.stat(self.tmpdir)
        self.assertTrue(filestat.st_mode & stat.S_IRUSR,
                         'USR does not have read access %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertTrue(filestat.st_mode & stat.S_IWUSR,
                         'USR does not have write access %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertTrue(filestat.st_mode & stat.S_IXUSR,
                         'USR does not have execute access %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IRGRP,
                         'GROUP should not have read access %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IWGRP,
                         'GROUP should not have write access %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IXGRP,
                         'GROUP should not have execute access %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IROTH,
                         'WORLD should not have read access %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IWOTH,
                         'WORLD should not have write access %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IXOTH,
                         'WORLD should not have execute access %o %s' 
                         % (filestat.st_mode, self.filepath)) 

        filestat = os.stat(self.filepath)
        self.assertTrue(filestat.st_mode & stat.S_IRUSR,
                         'USR does not have read access: %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertTrue(filestat.st_mode & stat.S_IWUSR,
                         'USR does not have write access: %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IXUSR,
                         'USR should not have execute access: %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IRGRP,
                         'GROUP should not have read access: %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IWGRP,
                         'GROUP should not have write access: %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IXGRP,
                         'GROUP should not have execute access: %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IROTH,
                         'WORLD should not have read access: %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IWOTH,
                         'WORLD should not have write access: %o %s' 
                         % (filestat.st_mode, self.filepath))
        self.assertFalse(filestat.st_mode & stat.S_IXOTH,
                         'WORLD should not have execute access: %o %s' 
                         % (filestat.st_mode, self.filepath)) 

    def testDictConfig( self):
        """
        Test that writing and reading a string returns the same string,
        and that a file with the correct protections is created with its backup
        """
        myc = managedDictClass()
        self.testconfig['integer']    = myc.integer
        self.testconfig['float']      = myc.float
        self.testconfig['string']     = myc.string
        self.testconfig['dictionary'] = myc.dictionary 
        
        self.testconfig.create_if_not_present()
        
        self.testconfig['integer']    = 0
        self.testconfig['float']      = 0.0
        self.testconfig['string']     = ''
        self.testconfig['dictionary'] = {}
        
        self.testconfig.read()
        
        self.assertEqual(myc.integer,    self.testconfig['integer']) 
        self.assertEqual(myc.float,      self.testconfig['float']) 
        self.assertEqual(myc.string,     self.testconfig['string']) 
        self.assertEqual(myc.dictionary, self.testconfig['dictionary']) 

    def testSlotsConfig( self):
        """
        Test that writing and reading a string returns the same string,
        and that a file with the correct protections is created with its backup
        """
        myc = managedSlotsClass()
        self.testconfig['integer']    = myc.integer
        self.testconfig['float']      = myc.float
        self.testconfig['string']     = myc.string
        self.testconfig['dictionary'] = myc.dictionary 
        
        self.testconfig.create_if_not_present()
        
        self.testconfig['integer']    = 0
        self.testconfig['float']      = 0.0
        self.testconfig['string']     = ''
        self.testconfig['dictionary'] = {}
        
        self.testconfig.read()
        
        self.assertEqual(myc.integer,    self.testconfig['integer']) 
        self.assertEqual(myc.float,      self.testconfig['float']) 
        self.assertEqual(myc.string,     self.testconfig['string']) 
        self.assertEqual(myc.dictionary, self.testconfig['dictionary']) 

if __name__ == '__main__':
    unittest.main()
    
