# Copyright 2015 Christian Kramer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import Enum, IntEnum


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

class OProfileType(Enum):
    BOOLEAN = "boolean"
    BYTE = "byte"
    SHORT = "short"
    INT = "int"
    LONG = "long"
    BYTES = "bytes"
    STRING = "string"
    RECORD = "record"
    STRINGS = "strings"
    DATETIME = "datetime" # treated as long
    BINARY = "binary" # (size:int)(bytes:bytes)
    EMBEDDED = "embedded" # (serialized_document:bytes)
    EMBEDDEDLIST = "embeddedlist" # (serialized_document:bytes)[{items}(data_type:byte)(data:data_type)]*
    EMBEDDEDSET = "embeddedset" # (serialized_document:bytes)[{items}(data_type:byte)(data:data_type)]*
    EMBEDDEDMAP = "embeddedmap" # [{header}(keyType:byte)(keyValue:keyType)]*[{values}(valueType:byte)(value:valueType)]*
    LINK = "link" # (cluster:int)(record:int)
    LINKLIST = "linklist" # (size:int)[{collection}(cluster:int)(record:int)]*
    LINKSET = "linkset" # (size:int)[{collection}(cluster:int)(record:int)]*
    LINKMAP = "linkmap" # [{values}(keyType:byte)(keyValue:keyType)(cluster:int)(record:int)]*
    DECIMAL = "decimal" # (scale:int)(valueSize:int)(value:bytes)
    # special types for special handling
    BYTE_STATIC = "byte_static" # used to represent something like (1) or (0)
    VARINT = "varint"
    # theres a special type of integer within serialization of records
    STRING_VARINT = "string-varint"

class OBinaryType(IntEnum):
    BOOLEAN = 0
    INTEGER = 1
    SHORT = 2
    LONG = 3
    FLOAT = 4
    DOUBLE = 5
    DATETIME = 6
    STRING = 7
    BINARY = 8
    EMBEDDED = 9
    EMBEDDEDLIST = 10
    EMBEDDEDSET = 11
    EMBEDDEDMAP = 12
    LINK = 13
    LINKLIST = 14
    LINKSET = 15
    LINKMAP = 16
    BYTE = 17
    TRANSIENT = 18
    DATE = 19
    CUSTOM = 20
    DECIMAL = 21
    LINKBAG = 22
    ANY = 23

class ORecordType(Enum):
    RAW_BYTES = 'b'
    FLAT = 'f'
    DOCUMENT = 'd'

class ORecordKind(Enum):
    NULL = -2
    RID = -3
    RECORD = 0

class OTXOperationType(Enum):
    UPDATES = 1
    DELETE = 2
    CREATIONS = 3

class OCommandClass(Enum):
    IDEMPOTENT = 'q'
    NON_IDEMPOTENT = 'c'
    SCRIPT = 's'

class OPlainClass(Enum):
    VERTEX = 'V'
    EDGE = 'E'

class ORidBagType(Enum):
    EMBEEDED = 1
    TREE = 2

class OSQLOperationType(Enum):
    CREATE = 1
    DELETE = 2
    DROP = 3
    UPDATE = 4
    MOVE = 5

class OSQLIndexType(Enum):
    UNIQUE = 'unique'
    NOTUNIQUE = 'notunique'
    FULLTEXT = 'fulltext'
