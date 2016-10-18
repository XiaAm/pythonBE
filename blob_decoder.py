# -*- coding: utf-8 -*-
"""
Created on Tue Oct 04 09:39:56 2016

@author: txia

Function to decode blob, not all of them are used.
The principal function is BlobToText()
"""

from blob_packing import ReservationData
from blob_packing import Header
from google.protobuf import text_format
import Model_pb2
import os

def dumpJsonList(json_list, jsonFilePath):
    f = open(jsonFilePath, 'w+')
    for ele in json_list:
        f.write(ele+'\n')
    f.close()

def blobtextToJsontext(blob):
    blobtext = BlobToText(blob)
    jsontext = textblobToJson(blobtext)
    return jsontext

def BlobToText(blob):
    # Extract the payload from the blob (i.e. unzip it and strip header)
    # Build a ReservationData
    res_data = ReservationData.deserialize(blob)
    # Unzip payload if needed and return (header, payload)
    header, payload = res_data.extract()

    # Extract structured message from protocol buffer's binary dump
    structured_model = Model_pb2.BomBEVoucher()
    structured_model.ParseFromString(payload)

    # format to ASCII text
    text = text_format.MessageToString(structured_model)
    return text

def textblobToJson(text):
    #transform the text that is converted from blob to json format
    lines = text.split('\n')
    res = map(transLine, lines)  
    result = ''
    # for each line in text, if it meets the following conition, 
    #then a comma ',' is added to the end of the line
    #    1.the following does not contain } 2.this line doesn't contain {
    for i in range(len(res)-2):
        if( ('}' not in res[i+1]) and ('{' not in res[i]) ):
            result = result + res[i] + ',\n'
        else:
            result = result + res[i] + '\n'             
    result = '{\n' + result + '}\n' + '}'
    
    result = result.replace('\n', '')    #remove \n, spark read.json dont recognize /n
    
    return result
def custF(s):
    #replace all "" in a line to "
    #replace all \' by ""+'    #s = s.replace("\\\'", "")
    #replace \+digits by ""+digit  -> combine these two, replace /
    while(s.find('\"\"')!=-1):
        s = s.replace('\"\"', '\"')
    s = s.replace("\\n", "") #this \n is not important even if removed, they are found in desp
    s = s.replace("\\\"", "") #the same, because we dont care about the description
    s = s.replace("\\", "")
    return s
def regulizeJsonList(json_list):
    return map(custF, json_list)    

def transLine(line):
    #transform one line of textblob to string which is in json format
    if(':' in line):
        #case 'chain_code: "BL"'
        res = []
        words = line.split(':')
        for word in words:
            word = word.strip()
            if( not(word.startswith('"') and word.endswith('"')) ):
                word = '"' + word + '"'
            res.append(word)
        return res[0] + ': ' + res[1]
    elif('{' in line):
        #case 'image {'
        #  voucher_creation_time {
        words = line.strip().split(' ')
        res = '"' + words[0] + '": ' + words[1]
        return res
    else:
        #  }
        return line

                
# 0.add , to the end of the proper lines (the line that should ends with , in json)   
"""
1.add "" to each field name (all of them donot have "")
1.add "" to those field values which donot have ""
make 1 to a single step, add "" to every word which doesnot have ""
2.add : in front of each { 
3.add { } to the beginning and end of the text
line[0:20]
['chain_code: "BL"', 'vid: 88583990', 'hosted_chain_code: "DHM"', 'property_city: "PAR"', 'property_id: "BLPARP06"', 'hosted_property_code: "BLPARP06"', 'hosted_brand_code: "BL"', 'property_country: "FR"', 'property_code: "P06"', 'image {', '  external_codes_context: "1A"', '  pnr_recloc: "5OU4C4"', '  confirmation_number: "88583990"', '  image_sequence_number: 0', '  max_stay_index: 1', '  booking_source: "12345675"', '  pnr_segment_tattoo: 1', '  transaction_id: 88583990000', '  request_id: 1717986918', '  is_force_sell: false']
"""

def test_transline():
    line1 = 'chain_code: "BL"'
    line2 = 'image {'
    print transLine(line1)
    print transLine(line2)
