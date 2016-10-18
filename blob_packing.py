#!/usr/bin/env python
"""
Blob tools. Contains the classes handling blob (un)packing
"""

import struct, zlib

class Header:
    """ Contains stuff for splitting/parsing/unzipping various "blob" data """

    # serialization method: big endian, no padding 
    _packer_format = '>BBBHI'

    def __init__(self):
        self.version = 0
        self.compression = 1
        self.message_version = 0
        self.blob_type = 0
        self.blob_size = None

    def serialize(self):
        """ Return a serialized version of the header """
        seralized_data = struct.pack(Header._packer_format,
                                      self.version,
                                      self.blob_type,
                                      self.compression,
                                      self.message_version,
                                      self.blob_size)
        return seralized_data

    @staticmethod
    def build_from(binary_header):
        """ Build a structured header from a serialized one """
        header = Header()
        (   header.version,
            header.blob_type,
            header.compression,
            header.message_version,
            header.blob_size  ) = struct.unpack(Header._packer_format,
                                                binary_header)
        return header


class ReservationData:
    """ Contains binary header + payload (maybe zipped) """

    def __init__(self):
        self.binary_header = None
        self.payload = None

    @staticmethod
    def build_with_header(structured_header, binary_model):
        """ Build with a user provided header """
        res_data = ReservationData()
        binary_header = structured_header.serialize()
        if structured_header.compression == 1 :
            payload = zlib.compress(binary_model)
        else:
            payload = binary_model
        res_data.binary_header = binary_header
        res_data.payload = payload
        return res_data

    @staticmethod
    def build_without_header(binary_model):
        """ Build with a generated header """
        structured_header = Header()
        structured_header.blob_size = len(binary_model)
        res_data = ReservationData.build_with_header(structured_header,
                                                     binary_model)
        return res_data

    def extract(self):
        """ Extract the structured header and payload """
        structured_header = Header.build_from(self.binary_header)
        if structured_header.compression == 1 :
            binary_model = zlib.decompress(self.payload)
        else:
            binary_model = self.payload
        return (structured_header, binary_model)

    def serialize(self):
        """ Return a serialized version of the reservation data """
        return self.binary_header + self.payload

    @staticmethod
    def deserialize(binary_res_data):
        """ Build from serialized data """
        res_data = ReservationData()
        res_data.binary_header = binary_res_data[0:9]
        res_data.payload = binary_res_data[9:]
        return res_data

class Toolbox: #pylint: disable=R0903,W0232
    """ Toolbox class (see individual methods doc) """
    @staticmethod
    def byte_to_hex(byte_str):
        """ Utility function do display binary data in hexadecimal format """
        return ''.join([ "%02X " % ord(x) for x in byte_str ]).strip()
