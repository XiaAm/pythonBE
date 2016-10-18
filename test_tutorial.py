# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 21:58:24 2016

@author: txia
test
"""


import params as pm
import sys
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import multiprocessing
import time

#parameters = pm.Parameters()
#
#es = Elasticsearch([{'host':parameters.es_host, 'port':parameters.es_port}])
#
#actions = []
#for j in range(0, 100):
#    #basic fields: index, docType, id, body (data and timestamp(optional))
#    action = {
#        "_index": "indexexamplebulk",
#        "_type": "ticket",
#        #"_id": j, #id is not mandatory
#        "_source": {
#            "any": "data"+str(j),
#            "timestamp": datetime.now()        
#        }
#    }
#    actions.append(action)
#    
#helpers.bulk(es, actions)


def cusFunction(x):
    time.sleep(0.0076) #simulate the processing time of data
    return x
    
l = range(1234)
def f0(l):
    #use iteration, slowest way
    time_a1 = datetime.now()
    print 'start.'
    l_mapped = []
    for ele in l:
        l_mapped.append(cusFunction(ele))
    time_a2 = datetime.now()
    diffTuple= divmod((time_a2-time_a1).total_seconds(), 60)
    print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )
    return l_mapped

def f0_1(l):
    time_a1 = datetime.now()
    print 'start.'
    l_mapped = [cusFunction(x) for x in l]
    time_a2 = datetime.now()
    diffTuple= divmod((time_a2-time_a1).total_seconds(), 60)
    print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )
    return l_mapped

def f1(l):
    time_a1 = datetime.now()
    print 'start.'
    l_mapped = map(cusFunction, l)
    time_a2 = datetime.now()
    diffTuple= divmod((time_a2-time_a1).total_seconds(), 60)
    print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )
    return l_mapped

def f2(l):
    time_a1 = datetime.now()
    print 'start.'
    #from multiprocessing.dummy import Pool as ThreadPool
    from multiprocessing import Pool as Pool
    pools = Pool(6)
    l_mapped = pools.map(cusFunction, l)
    time_a2 = datetime.now()
    diffTuple= divmod((time_a2-time_a1).total_seconds(), 60)
    print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )
    return l_mapped
    
#p0 = f0(l)
#p1 = f1(l)
    
#if __name__ == '__main__':
#    p = multiprocessing.Pool(6)
#    #from test_tutorial import cusFunction
#    p00 = p.map(cusFunction, l)
#    del p
    

if __name__ == '__main__':    
    p00 = f0(l)    
    del p00
    p0_1 = f0_1(l)
    del p0_1
    p11 = f1(l)
    del p11    
    
    p22 = f2(l)
    del p22
    del l
    