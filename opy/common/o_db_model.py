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
from io import BytesIO
import struct

from opy.common.o_db_constants import ORidBagType, OConst, ORecordType, ORecordKind
from opy.database.o_db_profile_parser import OProfileParser, OElement, OGroup


__author__ = 'daill'

class ORidBagBinary(object):
    def __init__(self):
        self.size = 0
        self.content = None
        self.entries = None

class ORidBagDocument(object):
    """
    If config
    Rid format (cluster-id:short)(cluster-position:long)
    """
    def __init__(self, type:ORidBagType):
        self.__config_str = "(config:byte)"
        # optional uuid
        self.__temp_uuid = "(most-sig-bits:long)(least-sig-bits:long)"
        self.__embedded_str = "(size:int)[{links}(cluster-id:short)(cluster-position:long)]*"
        self.__tree_str = "(fileId:long)(pageIndex:long)(pageOffset:int)(size:int)(changesSize:int)[[{links}(cluster-id:short)(cluster-position:long)](changeType:byte)(value:int)]*"

        self.__temp_uuid_profile = None
        self.__config_profile = None
        self.__embedded_profile = None
        self.__tree_profile = None

        self.__ridbagtype = type

    def getconfigprofile(self):
        if self.__config_profile is None:
            profile_parser = OProfileParser()
            self.__config_profile = profile_parser.parse(self.__config_str)

        return self.__config_profile

    def getuuidprofile(self):
        if self.__temp_uuid_profile is None:
            profile_parser = OProfileParser()
            self.__temp_uuid_profile = profile_parser.parse(self.__temp_uuid)

        return self.__temp_uuid_profile

    def getembeddedprofile(self):
        if self.__embedded_profile is None:
            profile_parser = OProfileParser()
            self.__embedded_profile = profile_parser.parse("{}{}".format(self.__config_str,self.__embedded_str))

        return self.__embedded_profile

    def gettreeprofile(self):
        if self.__tree_profile is None:
            profile_parser = OProfileParser()
            self.__tree_profile = profile_parser.parse(self.__tree_str)

        return self.__tree_profile

    def decode(self, unpack_data, data):
        """
        Decodes the embedded variant of a ridbag

        :param unpack_data:
        :param data:
        :return:
        """
        data_dict = {}
        rest = data
        num_repeats = 0

        def processelement(element: OElement):
            nonlocal rest, data_dict

            if isinstance(element, OGroup):
                nonlocal num_repeats

                # save main state
                main_dict = data_dict

                main_dict[element.name] = list()

                while (num_repeats > 0):
                    data_dict = {}
                    for sub_element in element.getelements():
                        rest = processelement(sub_element)
                    num_repeats -= 1
                    main_dict[element.name].append(data_dict)

                data_dict = main_dict
            else:
                # handling of a term
                rest, value = unpack_data(element.type, rest, name=element.name)
                data_dict[element.name] = value

                if element.name == 'size':
                    num_repeats = value
            return rest

        def processprofile(elements):
            """
            Iterate of the whole set of profile elements and unpack them
            :param elements:
            :return:
            """
            for element in elements:
                processelement(element)

            return OConst.OK

        if ORidBagType.EMBEEDED == self.__ridbagtype:
            status = processprofile(self.getembeddedprofile().getelements())
        elif ORidBagType.TREE == self.__ridbagtype:
            status = processprofile(self.gettreeprofile().getelements())

        # return the status (OK|Error) to decide what to do next and the extracted data
        return data_dict, status


class OTXEntry(object):
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

    def getdata(self):
        return self._data

    def getprofile(self):
        return self._request_profile


class OSQLPayload(object):
    """
    Base class for any SQLCommand.
    """
    def __init__(self):
        self.__data_dict = dict()

    def getdata(self):
        return self.__data_dict

    def getprofilestr(self):
        return self.__sql_profile_str


class OSQLClassName(OSQLPayload):
    """
    Represents a SQLCommand. The fetchplan is only used in case of a select command, otherwise its empty.

    See https://github.com/orientechnologies/orientdb/wiki/Fetching-Strategies
    """
    def __init__(self, text:str, non_text_limit:int, fetchplan:str, serialized_params:str):
        super().__init__()
        self._OSQLPayload__sql_profile_str = "(text:string)(non-text-limit:int)(fetchplan:string)(serialized-params:bytes)"
        self._OSQLPayload__data_dict = {"text":text,
                                        "non-text-limit":non_text_limit,
                                        "fetchplan":fetchplan,
                                        "serialized-params":serialized_params}


class OSQLCommand(OSQLPayload):
    """
    Represents a SQLCommand. The fetchplan is only used in case of a select command, otherwise its empty.

    See https://github.com/orientechnologies/orientdb/wiki/Fetching-Strategies
    """
    def __init__(self, text:str, non_text_limit:int, fetchplan:str, serialized_params:str):
        super().__init__()
        self._OSQLPayload__sql_profile_str = "(text:string)(non-text-limit:int)(fetchplan:string)(serialized-params:bytes)"
        self._OSQLPayload__data_dict = {"text":text,
                                        "non-text-limit":non_text_limit,
                                        "fetchplan":fetchplan,
                                        "serialized-params":serialized_params}


class OSQLScriptCommand(OSQLPayload):
    """
    Represents a SQLScriptCommand. The fetchplan is only used in case of a select command, otherwise its empty.

    See https://github.com/orientechnologies/orientdb/wiki/Fetching-Strategies
    """
    def __init__(self, language:str, text:str, non_text_limit:int, fetchplan:list, serialized_params:str):
        super().__init__()
        self._OSQLPayload__sql_profile_str = "(language:string)(text:string)(non-text-limit:int)(fetchplan:string)(serialized-params:bytes)"
        self._OSQLPayload__data_dict = {"language":language,
                                        "text":text,
                                        "non-text-limit":non_text_limit,
                                        "fetchplan":fetchplan,
                                        "serialized-params":serialized_params}


class OTXOperationCreate(OTXEntry):
    """
    For temporary RID's in transactions we have to use -1 as clusterid and <-1 for clusterposition
    """
    def __init__(self, record_type:ORecordType, record_content:bytes, record_id:int):
        super().__init__(3, record_type, -1, record_id)
        self._request_profile = self._request_base_profile + "(record-content:bytes)"
        self._data.update({"record-content": record_content})


class OTXOperationDelete(OTXEntry):
    def __init__(self, record_type:ORecordType, cluster_id:int, cluster_position:int, version:int):
        super().__init__(2, record_type, cluster_id, cluster_position)
        self._request_profile = self._request_base_profile + "(version:int)"
        self._data.update({"version": version})


class OTXOperationUpdate(OTXEntry):
    def __init__(self, record_type:ORecordType, cluster_id:int, cluster_position:int, version:int, content_changed:bool, record_content:bytes):
        super().__init__(1, record_type, cluster_id, cluster_position)
        self._request_profile = self._request_base_profile + "(version:int)(record-content:bytes)"
        self._data.update({"version": version,
                           "content-changed": content_changed,
                           "record-content": record_content})


class OVarInteger(object):
    """
    Class to provide methods to create unsigned varints
    See https://developers.google.com/protocol-buffers/docs/encoding?csw=1 and OVarIntSerializer

    Code partially taken from https://gist.github.com/nickelpro/7312782
    """
    def __init__(self):
        pass

    def signedtounsigned(self, value):
        # val = ((value ^ ~value)<<1) ^ (value >> 63)
        val = (value << 1) ^ (value >> 63)
        if (val == 0xfffffffffffffffe):
            val = -2
        return val

    def decode(self, buffer):
        total = 0
        shift = 0
        pos = 0
        bbuff = BytesIO(buffer)
        while True:
            val = struct.unpack('B', bbuff.read(1))[0]
            pos+=1
            if (val & 0x80) == 0x00:
                break
            total |= ((val & 0x7F) << shift)

            shift += 7
            if shift > 63:
                raise Exception("varint too long")

        raw = total | (val << shift)
        temp = ((raw % -2) ^ raw) >> 1
        return pos, temp ^ (total & -(1 << 63))

    def encode(self, value):
        _value = self.signedtounsigned(value)
        total = b''
        while (_value & 0xFFFFFFFFFFFFFF80) != 0x00:
            bits = (_value & 0x7F | 0x80)
            _value >>= 7
            if _value == -1:
                _value = 0x1FFFFFFFFFFFFFF
            total += struct.pack('B', bits)
        bits = _value & 0x7F
        total += struct.pack('B', bits)
        return total


class ORecord(object):
    """
    This class represents a record.

    -3 for RID
    -2 for null record
    Everything else for a record
    """
    def __init__(self, kind:ORecordKind):
        self.__kind = kind

        self.__rid_profile_str = "(cluster-id:short)(cluster-position:long)"
        self.__record_profile_str = "(record-type:byte)(cluster-id:short)(cluster-position:long)(record-version:int)(record-content:bytes)"
        self.__record_profile = None
        self.__rid_profile = None

    def getresponseprofile(self):
        profile_parser = OProfileParser()

        if self.__kind == ORecordKind.NULL:
            # do nothing return empty profile
            return profile_parser.parse("")
        elif self.__kind == ORecordKind.RID:
            if self.__rid_profile is None:
                self.__rid_profile = profile_parser.parse(self.__rid_profile_str)
            return self.__rid_profile
        else:
            if self.__record_profile is None:
                self.__record_profile = profile_parser.parse(self.__record_profile_str)
            return self.__record_profile