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
import logging
import inspect
import binascii
import struct
from common.o_db_exceptions import SerializationException
from database.o_db_codec import OCodec
from database.o_db_constants import OConst, ORidBagType
from database.o_db_profile_parser import OProfileParser, OElement, OGroup
from database.protocol.o_op_request import ORidBag

__author__ = 'daill'

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
        bbuff = BytesIO(buffer)
        while True:
            val = struct.unpack('B', bbuff.read(1))[0]
            if (val & 0x80) == 0x00:
                break
            total |= ((val & 0x7F) << shift)

            shift += 7
            if shift > 63:
                raise Exception("varint too long")

        raw = total | (val << shift)
        temp = ((raw % -2) ^ raw) >> 1
        return temp ^ (total & -(1 << 63))

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


class OSerializer(object):
    entities = None

    def encode(self, data):
        raise NotImplementedError("You have to implement the encode method")

    def decode(self, data):
        raise NotImplementedError("You have to implement the decode method")


class OBinarySerializer(OSerializer):
    """
    This class provides all necessary code to decode a binary encoded record and vice versa with the following structure:

    version: (version:byte)
    class name: (className:string)
    header (repeating until field_name_length is 0): [{header}(field_name_length:int)(field_name:string)(pointer_to_data:int)(data_type:byte)]*
    data: depends on the type of data and position

    """
    def __init__(self):
        self.__codec = OCodec()
        self.__schema_profile_str = "(version:byte)(className:string-varint)"
        self.__schema_profile = None
        self.__header_profile_str = "[{header}(field_name_length:varint)(field_name:string)(pointer_to_data:int)(data_type:byte)]*"
        self.__header_profile = None
        # this will be computed on the fly
        self.__data_profile = None

    def encode(self, data):
        pass

    def decode(self, data):
        # parse the static header profiles
        if not self.__schema_profile:
            self.__schema_profile = OProfileParser().parse(self.__schema_profile_str)

        if not self.__header_profile:
            self.__header_profile = OProfileParser().parse(self.__header_profile_str)

        data_dict = {}
        error_state = False
        rest = data
        group_repeat = 0

        def processelement(element: OElement):
            nonlocal rest, data_dict, group_repeat

            if isinstance(element, OGroup):

                # save main state
                main_dict = data_dict

                main_dict[element.name] = list()

                while (group_repeat > 0):
                    data_dict = {}
                    for sub_element in element.getelements():
                        rest = processelement(sub_element)
                    group_repeat -= 1
                    main_dict[element.name].append(data_dict)

                data_dict = main_dict
            else:
                # handling of a term
                rest, value = self.__codec.unpackdata(element.type, rest, name=element.name)

                if element.name == "num-cfg-items":
                    # save value as indicator how often the following group will be repeated
                    group_repeat = value

                data_dict[element.name] = value
            return rest

        def processprofile(elements):
            """
            Iterate of the whole set of profile elements and unpack them
            :param elements:
            :return:
            """
            for element in elements:
                # fetch an error if it occurs
                nonlocal error_state
                if error_state:
                    return OConst.ERROR

                processelement(element)

            return OConst.OK


        status = processprofile(self.__schema_profile.getelements())

class OCSVSerializer(OSerializer):
    def __init__(self):
        pass

    def encode(self, data):
        pass

    def decode(self, data):
        # decode
        decoded_str = data.decode("utf8")
        # split by @ to retrieve name of class and separated list of fields
        base_split = decoded_str.split('@')

        if len(base_split) == 2:
            class_name = base_split[0]
            target_module = inspect.importlib.import_module(self.entities[class_name])
            # instantiiate object by class name
            targetClass = getattr(target_module, class_name)
            instance = targetClass()
            linkdict = dict()

            field_list_str = base_split[1]

            field_list = field_list_str.split(',')

            logging.debug("extracted field list {}".format(field_list))

            for field in field_list:
                field_split = field.split(':')
                if len(field_split) == 2:
                    field_name = field_split[0].strip(' ')
                    if field_split[1].find('"') != -1:
                        field_value = field_split[1].strip('" ')

                        if hasattr(instance, field_name):
                            setattr(instance, field_name, field_value)

                        logging.debug("parse field {} with value {}".format(field_name, field_value))
                    else:
                        # possible base64 string
                        field_value = field_split[1].strip('%; ')
                        base64_binary = binascii.a2b_base64(field_value)
                        logging.debug("decoded base64: " + str(base64_binary))


                        parser = OCodec()
                        data_dict = parser.decode(ORidBag(ORidBagType.EMBEEDED), base64_binary)

                        logging.debug("decoded base64: " + str(base64_binary))

                        # map ids to instance
                        instance.linkdict[field_name] = data_dict['links']

                        logging.debug("parsed data from base64: {}".format(data_dict))


                else:
                    raise SerializationException("could not split '{}' by :".format(field))

            return instance
        else:
            raise SerializationException("could not split record content '{}' by @".format(decoded_str))

