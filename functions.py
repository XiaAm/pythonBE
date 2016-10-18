# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 07:20:09 2016

@author: txia
functions
"""
from datetime import datetime
import pandas as pd
import params
import json
##### functions for blob
p = params.Parameters()
tableIATA = pd.read_csv(filepath_or_buffer=p.path_IATAinfo, header=None, names=p.fields_IATAinfo)
# table of property information that found in blob 08-26 (whole day)
table_propertyInfo = pd.read_csv(filepath_or_buffer=p.path_propertyInfo_0826blob, header=None, names=p.fields_propertyInfo)

################         functions for decoded blob
def lineClean(line):
    #replace all "" in a line to "
    #replace all \' by ""+'    #s = s.replace("\\\'", "")
    #replace \+digits by ""+digit  -> combine these two, replace /
    while(line.find('\"\"')!=-1):
        line = line.replace('\"\"', '\"')
    line = line.replace("\\n", "") #this \n is not important even if removed, they are found in desp
    line = line.replace("\\\"", "") #the same, because we dont care about the description
    line = line.replace("\\", "")
    return line
    #still 25 not good, because some quote is removed
    #like this: {"text": "RQ K-BED NSMK FLOOR  },"transaction_id":
    
def getRating(table_propertyInfo, propertyId):
    rating = table_propertyInfo.loc[table_propertyInfo['property_id']==propertyId]['rating']
    if(rating.shape[0]==1):
        return str(rating.values[0])
    else:
        return '-1'
    
def get_coorOfPropertyId(table_propertyInfo, propertyId):
    res_lat = table_propertyInfo.loc[table_propertyInfo['property_id']==propertyId]['latitude']
    if(res_lat.shape[0]==1):
        res_long = table_propertyInfo.loc[table_propertyInfo['property_id']==propertyId]['longitude']
        return str(res_lat.values[0])+','+str(res_long.values[0])
    else:
        return '-80.00000, 180.00000'
        
def get_coor(dataFrame_pandas, iata):
    res_lat = dataFrame_pandas.loc[dataFrame_pandas['iata']==iata]['lat']
    if(res_lat.shape[0]==1):
        res_long = dataFrame_pandas.loc[dataFrame_pandas['iata']==iata]['long']
        return str(round(res_lat.values[0], 5))+','+str(round(res_long.values[0], 5))
    else:
        return '-80.00000, 180.00000'
        
def formatTransactionDate(year,month,day,hour,minute,second):
    #yyyy-MM-dd HH:mm:ss, year,month...are string
    if(len(month) == 1): month = '0'+month
    if(len(day) == 1): day = '0'+day
    if(len(hour) == 1): hour = '0'+hour
    if(len(minute) == 1): minute = '0'+minute
    if(len(second) == 1): second = '0'+second
    return year+'-'+month+'-'+day+' '+hour+':'+minute+':'+second

def formatCheckDate(year,month,day):
    #yyyy-MM-dd, year,month,day are string
    if(len(month) == 1): month = '0'+month
    if(len(day) == 1): day = '0'+day
    return year+'-'+month+'-'+day
    
def formatTransactionDate_fromDict(transaction_time_dict):
    year = transaction_time_dict['year'].encode('ascii')
    month = transaction_time_dict['month'].encode('ascii')
    day = transaction_time_dict['day'].encode('ascii')
    hour = transaction_time_dict['hour'].encode('ascii')
    minute = transaction_time_dict['minute'].encode('ascii')
    second = transaction_time_dict['second'].encode('ascii')
    return formatTransactionDate(year,month,day,hour,minute,second)
    
def formatCheckDate_fromDict(check_date_dict):
    year = check_date_dict['year'].encode('ascii')
    month = check_date_dict['month'].encode('ascii')
    day = check_date_dict['day'].encode('ascii')
    return formatCheckDate(year,month,day)
    
def formatDateTime_fromDict(transaction_time_dict):
    year = int(transaction_time_dict['year'].encode('ascii') )
    month = int(transaction_time_dict['month'].encode('ascii') )
    day = int(transaction_time_dict['day'].encode('ascii') )
    hour = int(transaction_time_dict['hour'].encode('ascii') )
    minute = int(transaction_time_dict['minute'].encode('ascii') )
    second = int(transaction_time_dict['second'].encode('ascii') )
    return datetime(year, month, day, hour, minute, second) 

def get_pnr_recloc_fromJson(blobJson):
    try:
        pnr_recloc = blobJson['image']['pnr_recloc']
    except KeyError:
        pnr_recloc = 'notFoundpnr_recloc'
    return pnr_recloc
    
def get_booking_code_fromJson(blobJson):
    try:
        booking_code = blobJson['image']['roomstay']['booking_code']
    except KeyError:
        booking_code = 'notFoundBookingCode'
    return booking_code
    
def get_productCode_fromJson(blobJson):
    try:
        product_code = blobJson['image']['roomstay']['product']['code']
    except KeyError:
        product_code = 'notFoundProductCode'
    return product_code
    
def get_rateCode_fromJson(blobJson):
    try:
        rate_code = blobJson['image']['roomstay']['rate_plan']['code']
    except KeyError:
        rate_code = 'notFoundRateCode'
    return rate_code
    
def get_reservCreationChannel_fromJson(blobJson):
    try:
        reservCreationChannel = blobJson['image']['reservation_creator_channel']
    except KeyError:
        reservCreationChannel = 'notFoundReservCreationChannel'
    return reservCreationChannel
    
def get_cityCode_fromJson(blobJson):
    try:
        cityCode = blobJson['image']['originator']['city_code'].encode('ascii')
    except KeyError:
        cityCode = 'notFoundCityCode'
    return cityCode

def get_amount_fromJson(blobJson):
    try:
        amount = blobJson['image']['consolidated_amounts']['global_total_amount_after_tax']['value'].encode('ascii')
        amount = float(amount)
    except KeyError:
        amount = -1.00000
    return amount
    
def get_originatorType_fromJson(blobJson):
    try:
        oType = blobJson['image']['originator']['originator_type'].encode('ascii')
        oType = int(oType)
    except KeyError:
        oType = -1
    return oType
    
def get_guestCount_fromJson(blobJson):
    try:
        gCount = blobJson['image']['roomstay']['product']['occupancy']['guest_count'].encode('ascii')
        gCount = int(gCount)
    except KeyError:
        gCount = -1
    return gCount
    
def get_peusdoCityCode_fromJson(blobJson):
    try:
        pseudoCityCode = blobJson['image']['originator']['pseudo_city_code'].encode('ascii')
    except KeyError:
        pseudoCityCode = 'notFoundCityCode'
    return pseudoCityCode
    
def get_propertyId_fromJson(blobJson):
    try:
        property_id = blobJson['property_id'].encode('ascii')        
    except KeyError:
        property_id = 'notFoundPropertyId'
        
    if(property_id=='notFoundPropertyId'):
        try:        
            CC = blobJson['chain_code'].encode('ascii')
            property_city = blobJson['property_city'].encode('ascii')
            property_code = blobJson['property_code'].encode('ascii')
            property_id = CC+property_city+property_code
        except KeyError:
            property_id = 'notFoundPropertyId'
    return property_id
def getOfficeId(blobJson):
    try:
        office_id = blobJson['image']['originator']['amadeus_office_id'].encode('ascii')
    except KeyError:
        office_id = 'notOfficeIdFound'
    return office_id    
def getChainCode(blobJson):
    try:
        office_id = blobJson['chain_code'].encode('ascii')
    except KeyError:
        office_id = 'notChainCodeFound'
    return office_id
def getPropertyCountry(blobJson):
    try:
        office_id = blobJson['property_country'].encode('ascii')
    except KeyError:
        office_id = 'notPropertyCountryFound'
    return office_id
def getLOS(checkinDateStr, checkoutDateStr):
    #yyyy-mm-dd
    d1 = datetime.strptime(checkinDateStr, '%Y-%m-%d')
    d2 = datetime.strptime(checkoutDateStr, '%Y-%m-%d')
    return str((d2-d1).days)
def getWeekdayFromDateStr(dateStr):
    d = datetime.strptime(dateStr, '%Y-%m-%d')    
    weekdayInt = d.weekday()
    dictWeekday = {
        0: "1Mon",
        1: "2Tue",
        2: "3Wed",
        3: "4Thu",
        4: "5Fri",
        5: "6Sat",
        6: "7Sun"
    }
    return dictWeekday.get(weekdayInt, 'noWeekday')
def deducedTripType(checkinDateStr, checkoutDateStr):
    # if the stay contains Saturday night, it is leisure, dont consider the guest count
    # otherwise it is business
    if(checkinDateStr=='noCheckinDateFound' or checkoutDateStr=='noCheckoutDateFound'):
        return 'business'
    d1 = datetime.strptime(checkinDateStr, '%Y-%m-%d')
    d2 = datetime.strptime(checkoutDateStr, '%Y-%m-%d')
    weekdayCheckin = d1.weekday()
    weekdayCheckout = d2.weekday()
    if((d2-d1).days >= 7):           #if LOS is bigger than 7(one week), it contains Saturday for sure
        return 'leisure'
    if(weekdayCheckout <= weekdayCheckin): #this case, stay contains Sunday for sure, than judge Saturday
        if(weekdayCheckin <= 5):      #if checkin is before or on Saturday  
            return 'leisure'
        else:
            return 'business'
    elif(weekdayCheckout==6):    #check out on Sunday
        return 'leisure'
    else:
        return 'business'

def ameliorateJson(r):
    #input a json, out a json in which additional fields are added 
    #r is string of blob, load it to a json, and store its useful information in a new json
    try:    
        r = json.loads(r)
        res = json.loads('{}')
        iata = r['property_city'].encode('ascii')
        loc = get_coor(tableIATA, iata)
        
        res['property_city'] = iata
        res['loc_propertyCity'] = loc  #add coordinate of property to json
        
        property_id = get_propertyId_fromJson(r)
        loc_propertyId = get_coorOfPropertyId(table_propertyInfo, property_id)
        res['property_id'] = property_id
        res['loc_propertyId'] = loc_propertyId
    
        pseudo_cityCode = get_peusdoCityCode_fromJson(r)
        loc_pseudoCity = get_coor(tableIATA, pseudo_cityCode)
        res['pseudo_city'] = pseudo_cityCode
        res['loc_pseudoCity'] = loc_pseudoCity    
               
        #add also coordinate of office to json
        officeCityCode = get_cityCode_fromJson(r)
        loc_office = get_coor(tableIATA, officeCityCode)
        res['office_city'] = officeCityCode
        res['loc_officeCity'] = loc_office
        
        res['originator_type'] = get_originatorType_fromJson(r)
        guestCountStr = get_guestCount_fromJson(r)
        res['guest_count'] = guestCountStr
        res['amount_after_tax'] = get_amount_fromJson(r)
        
        res['transaction_time'] = formatTransactionDate_fromDict(r['image']['distrib_history_status']['transaction_time'])
        #r['end_of_transaction_time'] = formatTransactionDate_fromDict(r['image']['distrib_history_status']['end_of_transaction_time'])
        #some blob dont have end_of_transaction_time, currently dont take this one into consideration        
        #r['_timestamp'] = formatDateTime_fromDict(r['image']['distrib_history_status']['transaction_time'])   #not used      
        
        checkin_dateStr = formatCheckDate_fromDict(r['image']['roomstay']['check_in_date'])
        res['checkin_date'] = checkin_dateStr        
        checkout_dateStr = formatCheckDate_fromDict(r['image']['roomstay']['check_out_date'])
        res['checkout_date'] = checkout_dateStr
        
        res['LOS'] = getLOS(checkin_dateStr, checkout_dateStr)
        res['checkin_weekday'] = getWeekdayFromDateStr(checkin_dateStr)
        res['checkout_weekday'] = getWeekdayFromDateStr(checkout_dateStr)        
        res['rating'] = getRating(table_propertyInfo, property_id)        
        res['deducedTripType'] = deducedTripType(checkin_dateStr, checkout_dateStr)
        
        res['pnr_recloc'] = get_pnr_recloc_fromJson(r)
        res['booking_code'] = get_booking_code_fromJson(r)
        res['roomTypeCode'] = get_productCode_fromJson(r)
        res['rate_code'] = get_rateCode_fromJson(r)
        res['reservCreationChannel'] = get_reservCreationChannel_fromJson(r)
                
        res['office_id'] = getOfficeId(r)
        res['chain_code'] = getChainCode(r)
        res['property_country'] = getPropertyCountry(r)
        return res
    except ValueError:
        return 'notGoodJsonFormat'
        #return #this return None
############    

def ascii_encode_dict(data):
    #http://stackoverflow.com/questions/9590382/forcing-python-json-module-to-work-with-ascii
    ascii_encode = lambda x: x.encode('ascii') if isinstance(x, unicode) else x
    return dict(map(ascii_encode, pair) for pair in data.items())

########### functions for avail RS, RQ
def formatGeoPoint(latitude, longitude):
    return str(latitude) + ',' + str(longitude)