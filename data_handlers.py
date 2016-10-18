# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 07:08:57 2016

@author: txia
manipulate the blob and the csv-like data, transform them to JSON, 
make them ready to be imported to ES by connectors.py
"""
import json
import functions as fs
import pandas as pd
from datetime import datetime
from data_source_merger import Merger
from multiprocessing import cpu_count as nbr_cpu

class BlobHandler():
    def __init__(self, path_blob):
        f = open(path_blob, 'r')
        self.lines = []
        for line in f:
            self.lines.append(line)
        f.close()
        # line is string
        print 'create BlobHandler.'
        
    def clean(self):
        print 'BlobHandle.clean start.'
        self.lines = map(fs.lineClean, self.lines)
        # line is still string
        print 'BlobHandle.clean finish.'
            
    def formatting(self, parallalized=False, nbrProcess=nbr_cpu()-2):
        time_a1 = datetime.now()
        print 'BlobHandle.formatting start.'
        if(not parallalized):
            self.lines = map(fs.ameliorateJson, self.lines)
        else:
            from multiprocessing import Pool 
            #not use from multiprocessing.dummy import Pool
            pools = Pool(processes=nbrProcess)
            print 'On the machine there are '+str(nbr_cpu())+' CPUs.'
            print 'BlobHandler creates '+str(nbrProcess)+' processes.'
            self.lines = pools.map(fs.ameliorateJson, self.lines)
        self.lines = [x for x in self.lines if x != 'notGoodJsonFormat'] #filter useless element in the list
        # line is json
        print 'BlobHandle.formatting finish.'
        time_a2 = datetime.now()
        diffTuple= divmod((time_a2-time_a1).total_seconds(), 60)
        print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )
    
    def getResult(self):
        return self.lines

class AvailRQHandler():
    def __init__(self, table_availRQ):
        self.table_availRQ = table_availRQ
        print 'create AvailRQHandler.'        
    def clean(self):
        print 'AvailRQHandler.clean start.'
        self.table_availRQ['guest_count'] = self.table_availRQ['guest_count'].astype('int')
        print 'AvailRQHandler.clean finish.'
    def formatting(self):
        print 'AvailRQHandler.formatting start.'
        print 'AvailRQHandler.formatting finish.'
    def getResult(self):
        return self.table_availRQ
    def getJsonList(self):
        print 'AvailRQHandler.getJsonList start.'
        res = []
        for row in self.table_availRQ.iterrows():
            #res.append(row[1].to_json())  #this is str
            res.append(json.loads(row[1].to_json(), object_hook=fs.ascii_encode_dict)) #this is json
        print 'AvailRQHandler.getJsonList finish.'
        return res

class AvailRSHandler():
    def __init__(self, table_availRS, table_hotelDespInfo, table_availRQ):
        self.table_availRS = table_availRS
        self.table_hotelDespInfo = table_hotelDespInfo
        self.table_availRQ = table_availRQ
        self.m = Merger(table_availRS)
        print 'create AvailRSHandler.'
    def clean(self):
        # remove the row of which checkin_date or checkout_date is noCheckinDateFound, noCheckoutDateFound
        print 'AvailRSHandler.clean start.'
        print 'AvailRSHandler.clean finish.'
    def formatting(self):
        print 'AvailRSHandler.formatting start.'
        #remove this step remove the availRS whose DCXid and seqNb are not found in availRQ (not too important, only less than 3%)
        self.m.merge(self.table_availRQ.loc[:, ['DCXid', 'seqNb', 'guest_count']], fields=['DCXid', 'seqNb'], how='inner') 
        self.m.merge(self.table_hotelDespInfo, fields=['property_id'], how='left') 
        self.table_availRS = self.m.getResult()  
        self.table_availRS['property_geoPoint'] = self.table_availRS.apply(lambda row: fs.formatGeoPoint(row['latitude'],row['longitude']), axis=1)
        self.table_availRS['tripType'] = self.table_availRS.apply(lambda row: fs.deducedTripType(row['checkin_date'], row['checkout_date']), axis=1)        
        self.table_availRS = self.table_availRS[(self.table_availRS.checkin_date!='noCheckinDateFound')
        &(self.table_availRS.checkout_date!='noCheckoutDateFound')]        
        self.table_availRS = self.table_availRS.reset_index()        
        print 'AvailRSHandler.formatting finish.'
    def getResult(self):
        return self.table_availRS
    def getJsonList(self):
        print 'AvailRSHandler.getJsonList start.'
        res = []
        for row in self.table_availRS.iterrows():
            res.append(row[1].to_json())
        print 'AvailRSHandler.getJsonList finish.'
        return res
        
class HotelDespInfoHandler():
    ## deal with availRQ, availRS, and hotelDescriptionInfo
    # path_singleAvailRQ, path_multiAvailRQ, path_singleAvailRS, path_multiAvailRS, path_propertyInfo
    def __init__(self, table_hotelDespInfo):
        self.table_hotelDespInfo = table_hotelDespInfo
        print 'create HotelDespInfoHandler.'               
    
    def clean(self):
        print 'HotelDespInfoHandler.clean start.'
        self.table_hotelDespInfo['latitude'] = self.table_hotelDespInfo['latitude'].astype('float')
        self.table_hotelDespInfo['longitude'] = self.table_hotelDespInfo['longitude'].astype('float')
        self.table_hotelDespInfo['rating'] = self.table_hotelDespInfo['rating'].astype('int')
        print 'GeneralHandler.clean finish.'
    
    def formatting(self):
        print 'HotelDespInfoHandler.formatting start.'
        print 'HotelDespInfoHandler.formatting finish.'
        
    def getResult(self):
        # return pandas table
        return self.table_hotelDespInfo

class roomTypeRatioHandler():
    def __init__(self, table_roomTypeRatio):
        self.table_roomTypeRatio = table_roomTypeRatio
        print 'create roomTypeRatioHandler.'
        
    def clean(self):
        print 'roomTypeRatioHandler.clean finish.'
        
    def formatting(self):
        print 'roomTypeRatioHandler.formatting finish.'
        
    def getResult(self):
        return self.table_roomTypeRatio
    def getJsonList(self):
        print 'roomTypeRatioHandler.getJsonList start.'
        res = []
        for row in self.table_roomTypeRatio.iterrows():
            res.append(row[1].to_json())
        print 'roomTypeRatioHandler.getJsonList finish.'
        return res