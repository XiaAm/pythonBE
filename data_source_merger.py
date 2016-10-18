# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 16:52:14 2016

@author: txia
"""

import os, json
import pandas as pd

class Merger():
    def __init__(self, mainTable):
        self.mainTable = mainTable
        print 'Merger created.'
    def merge(self, tableToBeMergered, fields, how):
        self.mainTable = pd.merge(self.mainTable, tableToBeMergered, on=fields, how=how)
    def getResult(self):
        return self.mainTable
  

# to be completed, code is in test.py
  
#class roomTypeMerge():
#    def __init__(self, table_singleAvailRS, table_reserv):
#        self.table_singleAvailRS = table_singleAvailRS
#        self.table_reserv = table_reserv
#    def merge(self):
#        
        
    