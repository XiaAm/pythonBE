# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 09:04:09 2016

@author: txia
test functions.py
"""

import functions as fs
from datetime import datetime
f1 = fs.formatDateTime

d1 = {}
d1['year'] = 2011
d1['month'] = 10
d1['day'] = 10
d1['hour'] = 12
d1['minute'] = 59
d1['second'] = 59
t1 = datetime(2011,10,10,12,59,59)
print t1
