# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 17:05:30 2016

@author: txia
"""

import sys
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import paramiko # library to do the SSH connection
import scp #library to do scp
import cx_Oracle #library to connect Oracle
import blob_decoder

class ElasticsearchConnector():
    def __init__(self, host, host_port):
        self.instance = Elasticsearch([{'host':host, 'port':host_port}])
        print 'ElasticsearchConnector initiation succeed.'
        
    def create_indices(self, index_name, mapping_of_this_index):
        #mapping is used when index is created.
        if not(self.instance.indices.exists(index_name)):
            self.instance.indices.create(index=index_name, body=mapping_of_this_index)
        print 'ElasticsearchConnector.create_indices finish.'
            
    #def send(self, indexInput, docTypeInput, idInput, body_json):
    #    self.instance.index(index=indexInput, doc_type=docTypeInput, id=idInput, body=body_json)
    def send(self, indexInput, docTypeInput, data):
        #data is a list of Json
        time_a1 = datetime.now()
        print 'ElasticsearchConnector.send start.'
        actions = []
        n = 0
        for elt in data:
            action = {
                "_index": indexInput,
                "_type": docTypeInput,
                #"_id": j, 
                #id is not mandatory
                "_source": elt
                    #"timestamp": datetime.now()   timestamp should directly be in elt, add it here isnot propre                  
            }
            actions.append(action)
#        try:
#            helpers.bulk(self.instance, actions)
#        except helpers.BulkIndexError:
#            n = n + 1
#        print 'number of helpers.BulkIndexError in helpers.bulk() is: '+str(n)
        helpers.bulk(self.instance, actions)
        print 'ElasticsearchConnector.send finish.'
        time_a2 = datetime.now()
        diffTuple= divmod((time_a2-time_a1).total_seconds(), 60)
        print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )
     
    # Main Function
    def execute(self, indexInput, mapping_of_this_index, docTypeInput, data):  #elasticsearch helpers??
        self.create_indices(indexInput, mapping_of_this_index)
        self.send(indexInput, docTypeInput, data)
        
class SSHConnector():
    def connect(self, param):
        try:
            self.param = param            
            self._client = paramiko.SSHClient()
            #add the following before connect, or it will raise exception 
            #paramiko.ssh_exception.SSHException: Server 'lgsap407.muc.amadeus.net' not found in known_hosts
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(hostname=param.logserverPRD_dict['hostname'], port=param.logserverPRD_dict['port'], 
                                 username=param.logserverPRD_dict['usrname'], password=param.logserverPRD_dict['pwd'])
            return self._client
        except paramiko.AuthenticationException:
            print 'SSH Authentication failed!\nPlease check username:'+param.MUCMSPOM_account_dict['usrname']+' and its password.'
            print 'The password expires every month within the current policy.'
            self._client.close()            
            raise
        except Exception, e:
            print e
            self._client.close()
            raise            
    def createSCPClient(self):
        #use sanitize function to allow wildcards
        #https://gist.github.com/stonefury/06ab3531a1c30c3b998a
        self.scp = scp.SCPClient(self._client.get_transport(), sanitize=lambda x: x)
    def scpExec(self, source, destination):
        #an example of source is '/ama/log_archive/PRD/hos/161001/feHOS_161001_19100*.gz'
        #or feHOS_161001_19100[0-9].gz, jsut like scp command
        time_a1 = datetime.now()
        print 'scpExec() start.'
        try:
            self.scp.get(source, destination, preserve_times=True)
        except Exception, e:
            #raise notch.agent.errors.DownloadError(str(e))
            print 'scpExec() has errors.\n'+e.message      
        print 'scpExec() finish.'
        time_a2 = datetime.now()
        diffTuple= divmod((time_a2-time_a1).total_seconds(), 60)
        print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )
    def scpOneHourPRD(self, startDate, startHour, destinationPrefix):
        #startDate DD/MM/YYYY, startHour HH24, copy the FE log between DD/MM/YYYY HH:00:00 to DD/MM/YYYY HH+1:00:00 
        #to local folder destinationPrefix+'/'+date
        #source example '/ama/log_archive/PRD/hos/161001/feHOS_161001_19*.gz'
        year = startDate[8:10]
        month = startDate[3:5]
        day = startDate[0:2]
        date = year+month+day
        source = '/ama/log_archive/PRD/hos/'+date+'/feHOS_'+date+'_'+str(startHour)+'*.gz' 
        destination = destinationPrefix+'/'+date
        self.scpExec(source, destination)
    def exec_cmd(self, cmd):
        #execute a command but not scp 
        try:
            return self._client.exec_command(cmd)
        except Exception, e:
            print e
            self._client.close()
            raise
    def close(self):
        try:
            self._client.close()
        except Exception, e:
            print e
            pass
        
class DBConnectors():
    def connect(self, db_parameters_dict):
        self.d=db_parameters_dict        
        self.dsn_tns=cx_Oracle.makedsn(self.d['ip'], self.d['port'], self.d['serviceName'])
        self.db=cx_Oracle.connect(self.d['usrname'], self.d['pwd'], self.dsn_tns)
        print 'Create connection to database successfully.'
    def exportBlobToText(self, startTime, endTime):
        #export the selected blob to text (list of string), Time format: DD/MM/YYYY HH24:MI:SS
        time_a1 = datetime.now()
        print 'exportBlobToText() start.'        
        cur = self.db.cursor()
        nb_result = cur.execute('select count(*) from DBAHOSPRD_P.evr_evoucherimage where d_transaction_type=\'sell   \' and d_transaction_status=\'commitd\' and d_transaction_time between to_date(\''+startTime+'\', \'DD/MM/YYYY HH24:MI:SS\') and to_date(\''+endTime+'\', \'DD/MM/YYYY HH24:MI:SS\')').next()[0]        
        print 'Number of blob found between this period from '+startTime+' to '+endTime+': '+str(nb_result)        
        
        blob_list = []   
        #here we can use the method fetchall() whose result is a list of tuples because it we are dealing LOB       
        #obliged to use iteration. If use fetchall(), it will cause cx_Oracle.ProgrammingError: LOB variable no longer valid after subsequent fetch
        for row in cur.execute('select reservation_data from DBAHOSPRD_P.evr_evoucherimage where d_transaction_type=\'sell   \' and d_transaction_status=\'commitd\' and d_transaction_time between to_date(\''+startTime+'\', \'DD/MM/YYYY HH24:MI:SS\') and to_date(\''+endTime+'\', \'DD/MM/YYYY HH24:MI:SS\')'):        
            #must manipulate LOB at first time, cannot put them in a list and manipulate them later            
            blob_list.append(row[0].read())
        blobText_list = map(blob_decoder.BlobToText, blob_list)
        
        print 'exportBlobToText() finish.'
        time_a2 = datetime.now()
        diffTuple= divmod((time_a2-time_a1).total_seconds(), 60)
        print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )
        return blobText_list
    def blobToJson(self, blobText_list):
        #transfer a list of decoded blob to a list of json(text)
        time_a1 = datetime.now()
        print 'blobToJson() start.'
        json_list = map(blob_decoder.textblobToJson, blobText_list)
        json_list = blob_decoder.regulizeJsonList(json_list)
        print 'blobToJson() finish.'
        time_a2 = datetime.now()
        diffTuple= divmod((time_a2-time_a1).total_seconds(), 60)
        print("Time consumed is: "+str(diffTuple[0])+" minutes and "+str(diffTuple[1])+" seconds." )
        return json_list
    def execute(self, param, startTime, endTime, jsonFilePath):
        #save all the blob of a period in json format in a single file
        #Time format: DD/MM/YYYY HH24:MI:SS
        self.connect(param.db_BTPRD_dict)
        blobText_list = self.exportBlobToText(startTime, endTime)
        json_list = self.blobToJson(blobText_list)
        blob_decoder.dumpJsonList(json_list, jsonFilePath)
        print 'DBconnectors,execute finished.'
        



