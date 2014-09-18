import socket
import logging
import struct
from enum import Enum
import sys


__author__ = 'daill'

class OModeInt(Enum):
    SYNCHRONOUS = 0
    ASYNCHRONOUS = 1

class OModeChar(Enum):
    SYNCHRONOUS = 's'
    ASYNCHRONOUS = 'a'

class OOperationType(Enum):
    REQUEST_SHUTDOWN = 1
    REQUEST_CONNECT = 2
    REQUEST_DB_OPEN = 3
    REQUEST_DB_CREATE = 4
    REQUEST_DB_EXIST = 6
    REQUEST_DB_DROP = 7
    REQUEST_CONFIG_GET = 70
    REQUEST_CONFIG_SET = 71
    REQUEST_CONFIG_LIST = 72
    REQUEST_DB_LIST = 74
    REQUEST_DB_CLOSE = 5
    REQUEST_DB_SIZE = 8
    REQUEST_DB_COUNTRECORDS = 9
    REQUEST_DATACLUSTER_ADD = 10
    REQUEST_DATACLUSTER_DROP = 11
    REQUEST_DATACLUSTER_COUNT = 12
    REQUEST_DATACLUSTER_DATARANGE = 13
    REQUEST_DATACLUSTER_COPY = 14
    REQUEST_DATACLUSTER_LH_CLUSTER_IS_USED = 16
    REQUEST_RECORD_METADATA = 29
    REQUEST_RECORD_LOAD = 30
    REQUEST_RECORD_CREATE = 31
    REQUEST_RECORD_UPDATE = 32
    REQUEST_RECORD_DELETE = 33
    REQUEST_RECORD_COPY = 34
    REQUEST_POSITIONS_HIGHER = 36
    REQUEST_POSITIONS_LOWER = 37
    REQUEST_RECORD_CLEAN_OUT = 38
    REQUEST_POSITIONS_FLOOR = 39
    REQUEST_COMMAND = 41
    REQUEST_POSITIONS_CEILING = 42
    REQUEST_TX_COMMIT = 60
    REQUEST_DB_RELOAD = 73
    REQUEST_PUSH_RECORD = 79
    REQUEST_PUSH_DISTRIB_CONFIG = 80
    REQUEST_DB_COPY = 90
    REQUEST_REPLICATION = 91
    REQUEST_CLUSTER = 92
    REQUEST_DB_TRANSFER = 93
    REQUEST_DB_FREEZE = 94
    REQUEST_DB_RELEASE = 95
    REQUEST_DATACLUSTER_FREEZE = 96
    REQUEST_DATACLUSTER_RELEASE = 97
    REQUEST_CREATE_SBTREE_BONSAI = 110
    REQUEST_SBTREE_BONSAI_GET = 111
    REQUEST_SBTREE_BONSAI_FIRST_KEY = 112
    REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR = 113
    REQUEST_RIDBAG_GET_SIZE = 114


class ODBType(Enum):
    DOCUMENT = "document"
    GRAPH = "graph"


class ODriver(Enum):
    DRIVER_NAME = 'OPy'
    DRIVER_VERSION = '0.1'


class OSerialization(Enum):
    SERIALIZATION_CSV = 'ORecordDocument2csv'
    SERIALIZATION_BINARY = 'ORecordSerializerBinary'


class OStorageTypes(Enum):
    PLOCAL = "plocal"
    MEMORY = "memory"


class OConst(Enum):
    SUCCESS_STATUS = "success_status"
    ERROR = "error"
    OK = "ok"
    EXCEPTION = "exception"
    EXCEPTION_MESSAGE = "exception-message"
    EXCEPTION_CLASS = "exception-class"


class OTypes(Enum):
    BOOLEAN = "boolean"
    BYTE = "byte"
    SHORT = "short"
    INT = "int"
    LONG = "long"
    BYTES = "bytes"
    STRING = "string"
    RECORD = "record"
    STRINGS = "strings"
    # special types for special handling
    BYTE_STATIC = "byte_static"

class ORecordType(Enum):
    RAW_BYTES = 'b'
    FLAT = 'f'
    DOCUMENT = 'd'

class ORecordKind(Enum):
    NULL = -2
    RID = -3
    RECORD = 0

