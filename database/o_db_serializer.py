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
from logging import info
import sys
from client.o_db_base import BaseVertex, BaseEntity

from common.o_db_exceptions import SerializationException, TypeNotFoundException
from database.o_db_codec import OCodec
from database.o_db_constants import ORidBagType, ODBType, OBinaryType
from common.o_db_model import ORidBagDocument, ORidBagBinary


__author__ = 'daill'


class OSerializer(object):
    entities = None

    def encode(self, data):
        raise NotImplementedError("You have to implement the encode method")

    def decode(self, data):
        raise NotImplementedError("You have to implement the decode method")

    def getinstance(self, class_name):
        try:
            logging.debug("create instance of class'{}'".format(class_name))

            # get target module
            target_module = inspect.importlib.import_module(self.entities[class_name])

            logging.debug("loaded module '{}'".format(target_module))

            # instantiiate object by class name
            targetClass = getattr(target_module, class_name)
            instance = targetClass()

            return instance
        except Exception as err:
            logging.error(err)

    def toobject(self, class_name, data):
        """
        Method to construct an object with the help of the given data dict. One field must have the key 'class-name' to determine
        the correct class to load.
        :param data:
        :return:
        """

        try:
            if class_name in self.entities:
                instance = self.getinstance(class_name)

                in_edges = dict()
                out_edges = dict()

                for field_name in data:
                    field_value = data[field_name]
                    if isinstance(field_value, ORidBagBinary):
                        if field_name.startswith("out_"):
                            if hasattr(instance, 'out_edges'):
                                out_edges[field_name[4:]] = field_value
                        else:
                            if hasattr(instance, 'in_edges'):
                                in_edges[field_name[3:]] = field_value
                    else:
                        if hasattr(instance, field_name):
                            setattr(instance, field_name, field_value)
                        else:
                            raise SerializationException("instance of class '{}' has no attribute with the name '{}'".format(class_name, field_name))

                    logging.debug("parse field {} with value {}".format(field_name, field_value))

                instance.in_edges = in_edges
                instance.out_edges = out_edges

                return instance
            else:
                raise SerializationException("there is no class with name '{}'".format(class_name))

        except Exception as err:
            logging.error(err)


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

    def encode(self, data:BaseVertex):
        if data:
            result = b''
            logging.debug("start binary serializing data: {}".format(data))

            # write version
            result += self.__codec.writebyte(0)

            # write class name
            class_name = data.__class__.__name__
            result += self.__codec.writestring(class_name)

            fields = data.persistent_attributes()
            value_bytes = b''
            value_pos = 0

            for field in fields:
                if hasattr(data, field):
                    value = getattr(data, field)
                    # check if the field is another vertex
                    try:
                        type = self.__codec.findotype(value)
                        value_bytes += self.__codec.writevalue(type, value)

                    except TypeNotFoundException as err:
                        logging.error(err)


                else:
                    logging.info("class '{}' has no attribute with name '{}'".format(class_name, field))

    def decode(self, data):
        if len(data) != 0:
            logging.debug("start binary deserializing bytes: {}".format(data))

            # start deserializing
            # first read byte
            version, rest = self.__codec.readbyte(data)

            # read class name
            class_name, rest = self.__codec.readvarintstring(rest)
            position_delta = 0

            record = dict()

            # read fields and pointers
            while True:
                length, rest = self.__codec.readvarint(rest)
                if length == 0:
                    break

                if length > 0:
                    field_name, rest = self.__codec.readbytes(length, rest)
                    pos, rest = self.__codec.readint(rest)
                    type, rest = self.__codec.readbyte(rest)
                else:
                    # TODO: Implement schema retrieval
                    # decode global property
                    logging.info("global property retrieval has not yet been implemented")
                    pass

                if pos != 0:
                    actual_position = self.__codec.position
                    self.__codec.position = 0

                    value, temp_rest = self.__codec.readvalue(type, data[pos:])
                    position_delta += self.__codec.position

                    self.__codec.position = actual_position

                    record[bytes.decode(field_name, 'utf-8')] = value

        return record, class_name

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

