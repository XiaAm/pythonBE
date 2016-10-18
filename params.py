# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 16:45:38 2016

@author: txia
"""

class Parameters:
    #List of parameters 
    #code path
    path_pythonBE = "d:\\PythonBE_2016_09_19"
    
    #path of some csv files of additional information 
    path_IATAinfo = 'd:\\city_names\\global_city.csv'
    fields_IATAinfo = ['id','airportName','city','country','iata','icao','lat','long','altitude','zoneUTC','dst','unkField']

    path_propertyInfo_0826blob = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\outputLog_forPropertyIdInBlob0826\\propertyInfo_forBlob160826.txt'
    path_propertyInfo_0826_20minavailRS = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\outputLog_propertyIdfoundInFilteredAvailRS1140to1200_toBeUsedByTTS\\propertyInfo_for160826_1140to59.txt'   
    fields_propertyInfo = ['property_id', 'latitude', 'longitude', 'rating']    
    
    #path of data sources    
    path_decodedBlob = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\blob_evr_160826_convertedToJson\\blobConvertedToJson.json'
    
    path_decodedBlob_1140to59 = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\blob_160826_1140to59_convertedToJson\\blobConvertedToJson.json'
    
    path_singleAvailRQ = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\0922_log0826_1140to59InCsv\\singleAvailRQ\\part-00000'
    path_multiAvailRQ = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\0922_log0826_1140to59InCsv\\multiAvailRQ\\part-00000' 
    
    path_singleAvailRS = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\0922_log0826_1140to59InCsv\\singleAvailRS\\part-00000'
    path_multiAvailRS = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\0922_log0826_1140to59InCsv\\multiAvailRS\\part-00000'

    path_reserv = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\0922_log0826_1140to59InCsv\\df_reserv\\part-00000'
    
    path_roomCategory = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\0922_log0826_1140to59InCsv\\roomCategory.txt'
    path_bedType = 'd:\\Userfiles\\txia\\Desktop\\dataUsedFrom0829\\0922_log0826_1140to59InCsv\\bedType.txt'
    
    es_host = 'localhost'
    es_port = 9200
    
    #Elasticsearch configuration    
    
    
    index_blobJson = 'blob160826withtriptype'
    docType_blobJson = 'reservation'
    mapping_blobJson = {
        "setting": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },      
        "mappings": {
            "reservation": {  #doc_type
                "properties": {
                    "property_city": {"type": "string", "index": "not_analyzed"},
                    "loc_propertyCity": {"type": "geo_point"},   #added, its mapping, because in json we can only store like string etc.
                    "vid": {"type": "long"},
                    "property_id": {"type": "string", "index": "not_analyzed"},
                    "loc_propertyId": {"type": "geo_point"},
                    "office_city": {"type": "string", "index": "not_analyzed"},
                    "loc_officeCity": {"type": "geo_point"}, #added
                    "pseudo_city": {"type": "string", "index": "not_analyzed"},
                    "loc_pseudoCity": {"type": "geo_point"}, #added 
                    "originator_type": {"type": "short"}, #added
                    "guest_count": {"type": "short"},   #added
                    "amount_after_tax": {"type": "float"},   #added
                    "transaction_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}, #added
                    "end_of_transaction_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}, #added
                    "checkin_date": {"type": "date", "format": "yyyy-MM-dd"},  #added
                    "checkout_date": {"type": "date", "format": "yyyy-MM-dd"},  #added
                    "LOS": {"type": "long"},
                    "rating": {"type": "short"},
                    "pnr_recloc": {"type": "string", "index": "not_analyzed"},
                    "booking_code": {"type": "string", "index": "not_analyzed"},
                    "roomTypeCode": {"type": "string","index": "not_analyzed"}, 
                    "rate_code": {"type": "string","index": "not_analyzed"},
                    "reservCreationChannel": {"type": "string","index": "not_analyzed"},
                    "office_id": {"type": "string", "index": "not_analyzed"},
                    "chain_code": {"type": "string", "index": "not_analyzed"},
                    "property_country": {"type": "string", "index": "not_analyzed"}
                }
                #,
                #"_timestamp": {
                #    "enable": True               
                #} #is no longer supported by ES, I use transaction_time as the timestamp in Kibana
            }
        }
    }
    
    ## field name is the same for both singleAvailRQ and multiAvailRQ    
    ## value of field isSingleAvailRQ is either isSingleRQ or isNotSingleRQ
    
    fieldName_availRQ = ['DCXid', 'seqNb', 'guest_count', 'office_id', 'organization',
                     'country', 'sapName', 'sapType', 'sapProduct', 'signValue', 'generateTime','isSingleAvailRQ'] 
    index_availRQ = 'availreqof08261140to59by0908'
    docType_availRQ = 'AvailRQ'
    mapping_availRQ = {
        "setting": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {    
            "AvailRQ": {
                "properties": {      
                    "DCXid": {"type": "string", "index": "not_analyzed"},
                    "seqNb": {"type": "string", "index": "not_analyzed"},
                    "guest_count": {"type": "short"},
                    "office_id": {"type": "string", "index": "not_analyzed"},
                    "organization": {"type": "string", "index": "not_analyzed"},
                    "country": {"type": "string", "index": "not_analyzed"},
                    "sapName": {"type": "string", "index": "not_analyzed"},
                    "sapType": {"type": "string", "index": "not_analyzed"},
                    "sapProduct": {"type": "string", "index": "not_analyzed"},
                    "signValue": {"type": "string", "index": "not_analyzed"},
                    "generateTime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                    "isSingleAvailRQ": {"type": "string", "index": "not_analyzed"}
                }    
            }
        }
    }
    
    # multi avail response
    fieldName_multiAvailRS = ['DCXid', 'seqNb', 'office_id', 'organization', 
    'country', 'city_code', 'property_id', 'chain_code', 'checkin_date', 'CI_day', 
    'checkout_date', 'CO_day', 'sapName', 'sapType', 'sapProduct', 'signValue', 'generateTime']
    index_multiAvailRS = 'availrsof08261140to59'
    docType_multiAvailRS = 'multiAvailRS'
    mapping_multiAvailRS = { 
        "setting": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {
            'multiAvailRS': {
                'properties': {
                    "DCXid": {"type": "string", "index": "not_analyzed"},
                    "seqNb": {"type": "string", "index": "not_analyzed"},
                    "office_id": {"type": "string", "index": "not_analyzed"},
                    "organization": {"type": "string", "index": "not_analyzed"},
                    "country": {"type": "string", "index": "not_analyzed"},
                    "city_code": {"type": "string", "index": "not_analyzed"},
                    "property_id": {"type": "string", "index": "not_analyzed"},
                    "chain_code": {"type": "string", "index": "not_analyzed"},
                    'checkin_date': {"type": "date", "format": "yyyy-MM-dd"},
                    "CI_day": {"type": "string", "index": "not_analyzed"},
                    'checkout_date': {"type": "date", "format": "yyyy-MM-dd"},
                    "CO_day": {"type": "string", "index": "not_analyzed"},
                    "sapName": {"type": "string", "index": "not_analyzed"},
                    "sapType": {"type": "string", "index": "not_analyzed"},
                    "sapProduct": {"type": "string", "index": "not_analyzed"},
                    "signValue": {"type": "string", "index": "not_analyzed"},
                    'generateTime': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                    'guest_count': {"type": "short"}, #this too
                    'property_geoPoint': {"type": "geo_point"},#this field is merged from another table
                    'rating': {'type': 'short'}#this too
                }    
            }
        }
    }
    
    # single avail response
    fieldName_singleAvailRS = ['DCXid', 'seqNb', 'office_id', 'organization', 
    'country', 'city_code', 'property_id', 'chain_code', 'checkin_date', 'CI_day', 
    'checkout_date', 'CO_day', 'sapName', 'sapType', 'sapProduct', 'signValue', 
    'bookingCode', 'roomType', 'roomTypeCode', 'rateCode', 'generateTime']
    index_singleAvailRS = 'availsinglersof08261140to59'
    docType_singleAvailRS = 'singleAvailRS'
    mapping_singleAvailRS = { 
        "setting": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {        
            'singleAvailRS': {
                'properties': {
                    "DCXid": {"type": "string", "index": "not_analyzed"},
                    "seqNb": {"type": "string", "index": "not_analyzed"},
                    "office_id": {"type": "string", "index": "not_analyzed"},
                    "organization": {"type": "string", "index": "not_analyzed"},
                    "country": {"type": "string", "index": "not_analyzed"},
                    "city_code": {"type": "string", "index": "not_analyzed"},
                    "property_id": {"type": "string", "index": "not_analyzed"},
                    "chain_code": {"type": "string", "index": "not_analyzed"},
                    'checkin_date': {"type": "date", "format": "yyyy-MM-dd"},
                    "CI_day": {"type": "string", "index": "not_analyzed"},
                    'checkout_date': {"type": "date", "format": "yyyy-MM-dd"},
                    "CO_day": {"type": "string", "index": "not_analyzed"},
                    "sapName": {"type": "string", "index": "not_analyzed"},
                    "sapType": {"type": "string", "index": "not_analyzed"},
                    "sapProduct": {"type": "string", "index": "not_analyzed"},
                    "signValue": {"type": "string", "index": "not_analyzed"},
                    'bookingCode': {"type": "string", "index": "not_analyzed"},
                    'roomType': {"type": "string", "index": "not_analyzed"},
                    'roomTypeCode': {"type": "string", "index": "not_analyzed"},
                    'rateCode': {"type": "string", "index": "not_analyzed"},
                    'generateTime': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                    'guest_count': {"type": "short"}, #from merge
                    'property_geoPoint': {"type": "geo_point"}, # from merge
                    'rating': {'type': 'short'} #fields added from merge
                }    
            }
        }
    }
    
    #is this reservation used? yes, this table is used to calculate the ratio that 
    # a product_code is booked
    fieldName_reserv = ['DCXid', 'chain_code', 'city_code', 'property_id', 
    'office_id', 'bookingCode', 'roomTypeCode', 'rateCode', 'sapName', 
    'sapType', 'sapProduct', 'signValue', 'generateTime']
    index_reserv = '0924_hbtlcrreserv'
    docType_reserv = 'reserv'
    mapping_reserv = { 
        "setting": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {        
            'reserv': {
                'properties': {
                    "DCXid": {"type": "string", "index": "not_analyzed"},
                    "chain_code": {"type": "string", "index": "not_analyzed"},
                    "city_code": {"type": "string", "index": "not_analyzed"},
                    "property_id": {"type": "string", "index": "not_analyzed"},
                    "office_id": {"type": "string", "index": "not_analyzed"},
                    'bookingCode': {"type": "string", "index": "not_analyzed"},
                    'roomType': {"type": "string", "index": "not_analyzed"},
                    'roomTypeCode': {"type": "string", "index": "not_analyzed"},
                    'rateCode': {"type": "string", "index": "not_analyzed"},
                    "sapName": {"type": "string", "index": "not_analyzed"},
                    "sapType": {"type": "string", "index": "not_analyzed"},
                    "sapProduct": {"type": "string", "index": "not_analyzed"},
                    "signValue": {"type": "string", "index": "not_analyzed"},
                    'generateTime': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}
                }    
            }
        }
    }
        
    #field of the table which shows the ratio that a roomTypeCode is booked
    fieldName_roomType = ['chain_code', 'roomTypeCode', 'NbrAvail', 'NbrReserv', 'ratio']
    index_RTratio = '0924_roomtyperatio'
    index_RTratio_chain = '0924_roomtyperatio_chain'
    docType_RTratio = 'rtratio'
    mapping_rtratio = { 
        "setting": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {        
            'rtratio': {
                'properties': {
                    'roomTypeCode': {"type": "string", "index": "not_analyzed"},
                    'NbrAvail': {"type": "long"},
                    'NbrReserv': {"type": "long"},
                    'ratio': {"type": "float"}
                }    
            }
        }
    }
    
    #field of the table 1.room category 2.amadeus bed type
    fieldName_roomCategory = ['newRoomCategory', 'newCode']
    index_roomCategory = '0924_roomcategory'
    docType_roomCategory = 'roomcategory'
    mapping_roomCategory = { 
        "setting": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {        
            'roomcategory': {
                'properties': {
                    'newRoomCategory': {"type": "string", "index": "not_analyzed"},
                    'newCode': {"type": "string", "index": "not_analyzed"}
                }    
            }
        }
    }
    fieldName_bedType = ['newBedType', 'newCode']
    index_bedType = '0924_bedtype'
    docType_bedType = 'bedtype'
    mapping_bedType = { 
        "setting": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {        
            'bedtype': {
                'properties': {
                    'newBedType': {"type": "string", "index": "not_analyzed"},
                    'newCode': {"type": "string", "index": "not_analyzed"}
                }    
            }
        }
    }
    

    logserverPRD_hostname = 'lgsap407.muc.amadeus.net'
    logserverPRD_port = 22
    logserverPRD_usrname = 'ptxia'
    logserverPRD_psw = 'Abc19911111'
    logserverPRD_feHOS_path = '/ama/log_archive/PRD/hos/' 
    #full path is path+date+fileName    
    #a complete example is /ama/log_archive/PRD/hos/160929/feHOS_160929_130329.gz
    log_output_folder = 'd:\\Userfiles\\txia\\Desktop\\test_connectors'
    
    MUCMSPOM_account_dict = {
        'usrname':'ptxia',
        'pwd':'Abc19911111'
    }
    
    logserverPRD_dict = {
        'hostname':'lgsap407.muc.amadeus.net',
        'port':22,
        'usrname':MUCMSPOM_account_dict['usrname'],
        'pwd':MUCMSPOM_account_dict['pwd'],
        'feHOS_path':'/ama/log_archive/PRD/hos/' 
    }   
    db_BTPRD_dict = {
        'ip':'172.31.7.37',
        'port':31317,
        'serviceName':'HOSBTPRD',
        'usrname':'hossup',
        'pwd':'11devhos11'
    }
    
#    logserverPDT_dict = { # not used
#        'hostname':'lgspdthos.muc.amadeus.net',
#        'port':22,
#        'usrname':MUCMSPOM_account_dict['usrname'],
#        'pwd':MUCMSPOM_account_dict['pwd'],
#        'feHOS_path':'??'   
#    }   
#    db_PDT_dict = { # not used
#        'ip':'172.17.197.134',
#        'port':33286,
#        'serviceName':'HOSPDT',
#        'usrname':'hosoncall',
#        'pwd':'11devhos11'
#    }   
    
    
    
    
   