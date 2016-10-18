# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 09:41:44 2016

@author: txia

an example of state machine design pattern
"""

class MyStateError(Exception):
    """
    Raised when an operation attemps an action that is not recommended at current state.
    """
    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg

class ManagerTutorial():
    CONST_NO_LOG_BLOB = 0
    CONST_NO_BLOB = 1
    CONST_LOG_READY = 2
    CONST_NO_LOG = 3 #this state (no log, but has blob) is never reached
    CONST_LOG_BLOB_READY = 4 #ready to be merged
    CONST_ACCOMPLISH = 5 #ready to be imported to elasticsearch
    state_dict = {
    0:"no log nor blob", 
    1:"have raw log but no blob",
    2:"have processed log but no blob",
    3:"have blob but no log",
    4:"have processed log and blob",
    5:"finishing merging log and blob and transfer them to Json"
    }    
    
    def __init__(self, period=7):
        self.period = period
        self.currentState = self.CONST_NO_LOG_BLOB
        
    def downloadLog(self):
        #copy log from log server
        print 'Current state is '+self.state_dict[self.currentState]+', execute downloadLog().'
        #check state        
        assert (self.currentState == 0), '[Error]Current state is '+self.state_dict[self.currentState]+', cannot execute downloadLog().'
        
        self.currentState = 1
        print 'downloadLog() finished.'
                    
    def processLog(self):
        #transfer log to csv
        print 'Current state is '+self.state_dict[self.currentState]+', execute processLog().'
        assert (self.currentState != 1), '[Error]Current state is '+self.state_dict[self.currentState]+', cannot execute processLog().'    

        self.currentState = 2
        print 'processLog() finished.'
            
    def downloadDecodedBlob(self):
        #import and decode blob from EVR database
        #in the architecture, this is two-step, in code here, it is one-step
        print 'Current state is '+self.state_dict[self.currentState]+', execute downloadDecodedBlob().'
        assert (self.currentState != 2), '[Error]Current state is '+self.state_dict[self.currentState]+', cannot execute downloadDecoderBlob().'
                
        self.currentState = 4
        print 'downloadDecoderBlob() finished.'
            
    def merge(self):
        #merge csv log, blob, other source(IATA, hotel cordinates...)
        #add new fields, modify fields (blob)
        #output Json file
        print 'Current state is '+self.state_dict[self.currentState]+', execute merge().'
        assert (self.currentState != 4), '[Error]Current state is '+self.state_dict[self.currentState]+', cannot execute merge().'

        self.currentState = 5
        print 'merge() finished.'
        
    
    def exportES(self):
        #export Json to elasticsearch
        print 'Current state is '+self.state_dict[self.currentState]+', execute exportES().'
        assert (self.currentState != 5), '[Error]Current state is '+self.state_dict[self.currentState]+', cannot execute exportES().'

        self.currentState = 0
        print 'exportES() finished.'
        
class TesterManagerTutorial():
    def execute(self):
        m = ManagerTutorial()
        m.downloadLog()
        m.processLog()
        m.downloadDecodedBlob()
        m.merge()
        m.exportES()
    
    
    
    
    
    
    
    
    