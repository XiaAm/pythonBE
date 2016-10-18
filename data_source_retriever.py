# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 14:56:51 2016

@author: txia

retrieve log file from log server

"""
import os, sys
import params as ps
from datetime import datetime
import connectors 

param = ps.Parameters()
class DataSourceRetriever():
    

    def copy_files_from_remote_to_local(self, source, destination):
        #use scp to copy the file 'source' to destination        
        sshconnector = connectors.SSHConnector(param)
        sshconnector.createSCPClient()
        sshconnector.scp(source, destination)
        sshconnector.close()

    def execute(self):
        print 'execute starts.'
        start_time = datetime.time()

        

        print 'execute ends.'
        end_time = datetime.time()

        diffTuple= divmod((end_time-start_time).total_seconds(), 60)
        print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )