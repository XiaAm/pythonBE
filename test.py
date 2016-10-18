# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 22:54:08 2016

@author: txia
test
"""


import connectors as ct
import params as pm
import pandas as pd
import data_handlers

#pms = pm.Parameters()
#connectEx = ct.ElasticsearchConnector(pms.es_host, pms.es_port)
#data = []
#mapping={}
#for i in range(0, 100):
#    data.append("{\"message\":{\"a\":\"asd\", \"temp_id\":"+str(i)+"}}")
#connectEx.execute("indexbulkex", mapping, "dd", data)


pms = pm.Parameters()
table_reserv= pd.read_csv(filepath_or_buffer=pms.path_reserv, header=None, names=pms.fieldName_reserv)
table_singleAvailRS = pd.read_csv(filepath_or_buffer=pms.path_singleAvailRS, header=None, names=pms.fieldName_singleAvailRS)

#result of group by
res_series = table_reserv.groupby(['chain_code', 'roomTypeCode'])['roomTypeCode'].count()
#rename it
res_series = res_series.rename('NbrReserv')
res_table = pd.DataFrame(res_series)
res_table.reset_index(inplace=True)

singleAvailRS_series = table_singleAvailRS.groupby(['chain_code', 'roomTypeCode'])['roomTypeCode'].count()
singleAvailRS_series = singleAvailRS_series.rename('NbrAvail')
singleAvailRS_table = pd.DataFrame(singleAvailRS_series)
singleAvailRS_table.reset_index(inplace=True)

ratio_roomType = pd.merge(singleAvailRS_table, res_table, on=['chain_code', 'roomTypeCode'], how='left')
def calRatio(a, b):
    if(a != a):  #judge if it is NaN
        return 0.0
    return float(a)/float(b)
def replaceNaNByZero(a):
    if(a != a):
        return 0.0
    return a
ratio_roomType['NbrReserv'] = ratio_roomType.apply(lambda row:replaceNaNByZero(row['NbrReserv']), axis=1)
ratio_roomType['ratio'] = ratio_roomType.apply(lambda row:calRatio(row['NbrReserv'], row['NbrAvail']), axis=1)


res1 = table_reserv.groupby(['roomTypeCode'])['roomTypeCode'].count()
res1 = res1.rename('NbrReserv')
res1_t = pd.DataFrame(res1)
res1_t.reset_index(inplace=True)

sAvailRS = table_singleAvailRS.groupby(['roomTypeCode'])['roomTypeCode'].count()
sAvailRS = sAvailRS.rename('NbrAvail')
sAvailRS_t = pd.DataFrame(sAvailRS)
sAvailRS_t.reset_index(inplace=True)

ratio_roomType_onlyRoomType = pd.merge(sAvailRS_t, res1_t, on=['roomTypeCode'], how='left')
ratio_roomType_onlyRoomType['NbrReserv'] = ratio_roomType_onlyRoomType.apply(lambda row:replaceNaNByZero(row['NbrReserv']), axis=1)
ratio_roomType_onlyRoomType['ratio'] = ratio_roomType_onlyRoomType.apply(lambda row:calRatio(row['NbrReserv'], row['NbrAvail']), axis=1)

d = data_handlers.roomTypeRatioHandler(ratio_roomType)
d.clean()
d.formatting()
json_list = d.getJsonList()
c = ct.ElasticsearchConnector(pms.es_host, pms.es_port)
c.execute(pms.index_RTratio_chain, pms.mapping_rtratio, pms.docType_RTratio, json_list)

d1 = data_handlers.roomTypeRatioHandler(ratio_roomType_onlyRoomType)
d1.clean()
d1.formatting()
json_list1 = d1.getJsonList()
c1 = ct.ElasticsearchConnector(pms.es_host, pms.es_port)
c1.execute(pms.index_RTratio, pms.mapping_rtratio, pms.docType_RTratio, json_list1)