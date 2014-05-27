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

import os
import os.path
import re
import shutil
import threading
import time

from vos import vos

class mirrorfile(object):
    """
    A contenxt manager that mirrors a file periodically to a destination 
    directory, which may be on disk or in a VOspace.
    """
    NO = -1
    IDLE = 0
    RUN = 1
    ENDING = 2
    
    def __init__(self, proxy=None, filepath=None, destpath=None, period=10.0):
        """
        Create a mirror object and verify that the destpath is a directory
        (container node in vos).  The filepath may not yet exist on creation,
        and might never exist.  This is not an error.
        
        The methods push and final_push are similar, but push only does a 
        single attempt to copy the file and does not verify success.  The
        final_push method does attempt to verify success and can be much
        more time consuming.
        
        The methods push and run are intended to be internal and should
        not normally be called directly.  Instead, mirrorfile should be used
        in a with statement of the form
        
            with mirrorfile(proxy=proxyfile, 
                            filepath=filepath, 
                            destpath=destpath):
                # do something to modify file
        
        If finer control over the push is necessary, perhaps because filepath
        will be deleted after use, the final_push method can be called 
        directly:
        
            with mirrorfile(proxy=proxypath, 
                            filepath=filepath, 
                            destpath=destpath) as mf:
                # create filepath
                # do something to modify file
                mf.final_push()
                # delete filepath
        
        Arguments:
        proxy: path to proxy file
        filepath: path to a file, which need not exist on creation
        destpath: path to a destination directory, or uri to a directory 
                  in a VOspace, which must already exist
        """
        self.filepath = filepath
        self.destpath = destpath
        self.use_vos = False
        if destpath:
            self.use_vos = (len(destpath) > 4 and destpath[:4] == 'vos:')
        self.state = mirrorfile.NO
        self.client = None
        self.period = period
        self.timer = None
        self.retry_limit = 10
        self.retry_period = period / self.retry_limit
        self.thread = None
        
        if self.destpath:
            if self.use_vos:
                if proxy:
                    self.client = vos.Client(proxy)
                else:
                    self.client = vos.Client()
                    
                if self.client.isdir(self.destpath):
                    self.state = mirrorfile.IDLE
                    self.timer = threading.Event()
                else:
                    self.client = None
            elif os.path.isdir(destpath):
                self.state = mirrorfile.IDLE
                self.timer = threading.Event()
    
    def push(self):
        """
        Push the file to its destination
        
        Arguments:
        <None>
        """
        if self.state == mirrorfile.IDLE and os.path.isfile(self.filepath):
            if self.use_vos:
                self.client.copy(self.filepath, self.destpath)
            else:
                shutil.copy2(self.filepath, self.destpath)
    
    def final_push(self):
        """
        Push the file to the destination, taking more care that the file 
        reaches its destination intact.
        
        Arguments:
        <None>
        """
        if self.state != mirrorfile.NO and os.path.isfile(self.filepath):
            if self.state == mirrorfile.RUN:
                self.state = mirrorfile.ENDING
                # cancel the timer if it is still running
                self.timer.set()

            if self.use_vos:
                retry = 0
                self.retry_timer.clear()
                while ((os.stat(self.filepath).st_size != 
                        self.client.copy(self.filepath, self.destpath)) and
                       retry < self.retry_limit):
                    retry += 1
                    self.timer.clear()
                    self.timer.wait(self.retry_period)
            else:
                shutil.copy2(self.filepath, self.destpath)
            
            self.state = mirrorfile.IDLE
    
    def __call__(self):
        """
        Periodically call push.  This method makes mirrorfile instances 
        callable so they can be called by threads.
        
        Arguments:
        <None>
        """
        if self.state != mirrorfile.NO:
            self.state = mirrorfile.RUN
            self.timer.clear()
            while self.state == mirrorfile.RUN:
                try:
                    self.push()
                    self.timer.wait(self.period)
                except Exception:
                    # In case of errors, just retry later
                    pass
    
    def __entry__(self):
        """
        Entry method for the condition handler
        
        Arguments:
        <none>
        """
        self.thread = threading.Thread(target=self)
        self.thread.start()
    
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit method for the condition handler, which kills and deletes the 
        thread.
        
        Arguments:
        <none>
        """
        self.final_push()
        
        
        
    
                
