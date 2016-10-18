# -*- coding: utf-8 -*-
"""
Created on Mon Oct 03 10:42:28 2016

@author: txia
test connectors.py
"""

import connectors
import params
import cx_Oracle
import blob_decoder

param = params.Parameters()


sc = connectors.SSHConnector()
sc.connect(param)

cmd = 'scp ptxia@lgsap407.muc.amadeus.net:/ama/log_archive/PRD/hos/160929/feHOS_160929_191257.gz d:\\Userfiles\\txia\\Desktop\\test_connectors'

sc.createSCPClient()
sc.scpExec('/ama/log_archive/PRD/hos/161001/feHOS_161001_19100[0-9].gz', param.log_output_folder)



#t=cur.execute('select count(*) from DBAHOSPRD_P.evr_evoucherimage where d_transaction_type=\'sell   \' and d_transaction_status=\'commitd\' and d_transaction_time between to_date(\'26/08/2016 11:40:00\', \'DD/MM/YYYY HH24:MI:SS\') and to_date(\'26/08/2016 12:00:00\', \'DD/MM/YYYY HH24:MI:SS\')')
#t2=cur.execute('select reservation_data from DBAHOSPRD_P.evr_evoucherimage where d_transaction_type=\'sell   \' and d_transaction_status=\'commitd\' and d_transaction_time between to_date(\'26/08/2016 11:59:00\', \'DD/MM/YYYY HH24:MI:SS\') and to_date(\'26/08/2016 12:00:00\', \'DD/MM/YYYY HH24:MI:SS\')')
   
"""
dbconnector = connectors.DBConnectors()
dbconnector.connect(param.db_BTPRD_dict)
startTime='26/08/2016 11:40:00'
endTime='26/08/2016 11:45:00'
blobtextlist = dbconnector.exportBlobToText(startTime, endTime)
json_list = dbconnector.blobToJson(blobtextlist)
jsonFilePath='d:\\Userfiles\\txia\\Desktop\\test_connectors\\blob20160826_1140to45.json'
blob_decoder.dumpJsonList(json_list, jsonFilePath)
"""

def test_execute():
    dbconnector = connectors.DBConnectors()
    startTime='26/08/2016 11:40:00'
    endTime='26/08/2016 11:45:00'
    jsonFilePath='d:\\Userfiles\\txia\\Desktop\\test_connectors\\blob20160826_1140to45.json'
    dbconnector.execute(param, startTime, endTime, jsonFilePath)   
#test_execute()