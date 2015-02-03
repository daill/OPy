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

import logging
import inspect
import binascii

from common.o_db_exceptions import SerializationException
from database.o_db_codec import OCodec
from database.o_db_constants import ORidBagType
from common.o_db_model import ORidBagDocument


__author__ = 'daill'


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
    class name: (className)(className:string)
    header (repeating until field_name_length is 0): [{header}(field_name_length:int)(field_name:string)(pointer_to_data:int)(data_type:byte)]*
    data: depends on the type of data and position

    """
    def __init__(self):
        self.__codec = OCodec()

    def encode(self, data):
        pass



    def decode(self, data):
        if len(data) != 0:
            # start deserializing
            # first read byte
            version, rest = self.__codec.readbyte(data)

            # read class name
            class_name, rest = self.__codec.readvarintstring(rest)

            # read fields and pointers
            while True:
                length, rest = self.__codec.readvarint(rest)
                if length == 0:
                    break
                field_name, rest = self.__codec.readvarintstring(rest)
                pos, rest = self.__codec.readint(rest)
                type, rest = self.__codec.readbyte(rest)


        pass


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
                        data_dict = parser.decode(ORidBagDocument(ORidBagType.EMBEEDED), base64_binary)

                        logging.debug("decoded base64: " + str(base64_binary))

                        # map ids to instance
                        instance.linkdict[field_name] = data_dict['links']

                        logging.debug("parsed data from base64: {}".format(data_dict))


                else:
                    raise SerializationException("could not split '{}' by :".format(field))

            return instance
        else:
            raise SerializationException("could not split record content '{}' by @".format(decoded_str))

