import socket
import logging
import struct
from enum import Enum
import sys
from database.o_db_constants import ORecordType


__author__ = 'daill'

class ODBTX(object):
    def __init__(self):
        pass

class OTXEntry:
    """
    Base class for all possible tx operations like CREATE, DELETE and UPDATE
    """
    def __init__(self, operation_type:bytes, record_type:ORecordType, cluster_id:int, cluster_position:int):
        self._request_base_profile = "(operation-type:byte)(cluster-id:short)(cluster-position:long)(record-type:byte)"
        self._request_profile = None
        self._data = {"operation-type": operation_type,
                       "cluster-id": cluster_id,
                       "cluster-position": cluster_position,
                       "begin": 1,
                       "record-type": record_type}

    def get_data(self):
        return self._data

    def get_profile(self):
        return self._request_profile

class OTXOperationCreate(OTXEntry):
    """
    For temporary RID's in transactions we have to use -1 as clusterid and <-1 for clusterposition
    """
    def __init__(self, record_type:ORecordType, record_content:bytes, record_id:int):
        super().__init__(3, record_type, -1, record_id)
        self._request_profile = self._request_base_profile + "(record-content:bytes)"
        self._data.update({"record-content": record_content})

class OTXOperationUpdate(OTXEntry):
    def __init__(self, record_type:ORecordType, cluster_id:int, cluster_position:int, version:int, content_changed:bool, record_content:bytes):
        super().__init__(1, record_type, cluster_id, cluster_position)
        self._request_profile = self._request_base_profile + "(version:int)(record-content:bytes)"
        self._data.update({"version": version,
                            "content-changed": content_changed,
                            "record-content": record_content})

class OTXOperationDelete(OTXEntry):
    def __init__(self, record_type:ORecordType, cluster_id:int, cluster_position:int, version:int):
        super().__init__(2, record_type, cluster_id, cluster_position)
        self._request_profile = self._request_base_profile + "(version:int)"
        self._data.update({"version": version})