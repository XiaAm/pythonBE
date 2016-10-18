# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 09:28:41 2016

@author: txia

main script: 
load data, format data and import data to ES
"""


import params as ps
import connectors
import data_handlers
import pandas as pd

p = ps.Parameters()
table_hotelDespInfo_20minavailRS = pd.read_csv(filepath_or_buffer=p.path_propertyInfo_0826_20minavailRS, header=None, names=p.fields_propertyInfo)
table_IATAinfo = pd.read_csv(filepath_or_buffer=p.path_IATAinfo, header=None, names=p.fields_IATAinfo)
table_singleAvailRQ = pd.read_csv(filepath_or_buffer=p.path_singleAvailRQ, header=None, names=p.fieldName_availRQ)
table_multiAvailRQ = pd.read_csv(filepath_or_buffer=p.path_multiAvailRQ, header=None, names=p.fieldName_availRQ)
table_availRQ = pd.concat([table_singleAvailRQ, table_multiAvailRQ])
#after concatenation, index should be reset, otherwise the index is not unique.
table_availRQ = table_availRQ.reset_index()


if __name__ == '__main__':
    # import blob
    blobHandles = data_handlers.BlobHandler(path_blob=p.path_decodedBlob)
    blobHandles.clean()
    blobHandles.formatting(True)
    json_list = blobHandles.getResult()

#c = connectors.ElasticsearchConnector(p.es_host, p.es_port)
#c.execute('0924_blob0826', p.mapping_blobJson, p.docType_blobJson, json_list)


"""
# import aiavlRQ
reqHandler = data_handlers.AvailRQHandler(table_availRQ)
reqHandler.clean()
reqHandler.formatting()
req_json = reqHandler.getJsonList()

c = connectors.ElasticsearchConnector(p.es_host, p.es_port)
#index name should be all lowercase
c.execute('0924_availrequest', p.mapping_availRQ, p.docType_availRQ, req_json)
"""


"""
# import availRS
table_singleAvailRS = pd.read_csv(filepath_or_buffer=p.path_singleAvailRS, header=None, names=p.fieldName_singleAvailRS)
table_multiAvailRS = pd.read_csv(filepath_or_buffer=p.path_multiAvailRS, header=None, names=p.fieldName_multiAvailRS)

#multi avail response
singleAvailHandler = data_handlers.AvailRSHandler(table_singleAvailRS, table_hotelDespInfo_20minavailRS, table_availRQ)
singleAvailHandler.clean()
singleAvailHandler.formatting()
sAvailRS_json = singleAvailHandler.getJsonList()
c = connectors.ElasticsearchConnector(p.es_host, p.es_port)
c.execute("0924_singleavailrs", p.mapping_singleAvailRS, p.docType_singleAvailRS, sAvailRS_json)


multiAvailHandler = data_handlers.AvailRSHandler(table_multiAvailRS, table_hotelDespInfo_20minavailRS, table_availRQ)
multiAvailHandler.clean()
multiAvailHandler.formatting()
mAvailRS_json = multiAvailHandler.getJsonList()
c = connectors.ElasticsearchConnector(p.es_host, p.es_port)
c.execute("0924_multiavailrs", p.mapping_multiAvailRS, p.docType_multiAvailRS, mAvailRS_json)
"""


"""
#import room category and bed type
table_roomCategory = pd.read_csv(filepath_or_buffer=p.path_roomCategory, header=None, names=p.fieldName_roomCategory)
r = data_handlers.roomTypeRatioHandler(table_roomCategory)
r.clean()
r.formatting()
c = connectors.ElasticsearchConnector(p.es_host, p.es_port)
c.execute(p.index_roomCategory, p.mapping_roomCategory, p.docType_roomCategory, r.getJsonList())

table_bedType = pd.read_csv(filepath_or_buffer=p.path_bedType, header=None, names=p.fieldName_bedType)
r1 = data_handlers.roomTypeRatioHandler(table_bedType)
r1.clean()
r1.formatting()
c1 = connectors.ElasticsearchConnector(p.es_host, p.es_port)
c1.execute(p.index_bedType, p.mapping_bedType, p.docType_bedType, r1.getJsonList())
"""