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

from client.o_db_base import BaseVertex, BaseEdge
from common.o_db_exceptions import SerializationException, TypeNotFoundException
from database.o_db_codec import OCodec
from common.o_db_constants import ORidBagType
from common.o_db_model import ORidBagDocument, ORidBagBinary


__author__ = 'daill'


class OSerializer(object):
    entities = None

    def __init__(self):
        self.entities = dict()
        self.createentitydict(BaseVertex)

    # build entity dict
    def createentitydict(self, base_class):
        subclasses = base_class.__subclasses__()
        for clazz in subclasses:
            self.entities[clazz.__name__] = clazz.__module__
            self.createentitydict(clazz)

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

                if isinstance(instance, BaseVertex):
                    in_edges = dict()
                    out_edges = dict()

                    for field_name in data:
                        field_value = data[field_name]
                        if isinstance(field_value, ORidBagBinary):
                            if field_name.startswith("out_"):
                                if hasattr(instance, 'out_edges'):
                                    out_edges[field_name[4:]] = self.toobject(field_name[4:], field_value)
                            else:
                                if hasattr(instance, 'in_edges'):
                                    in_edges[field_name[3:]] = self.toobject(field_name[3:], field_value)
                        else:
                            if hasattr(instance, field_name):
                                setattr(instance, field_name, field_value)
                            else:
                                raise SerializationException("instance of class '{}' has no attribute with the name '{}'".format(class_name, field_name))

                        logging.debug("parse field {} with value {}".format(field_name, field_value))

                    instance.in_edges = in_edges
                    instance.out_edges = out_edges

                    return instance
                elif isinstance(instance, BaseEdge):
                    edge_list = list()
                    if isinstance(data, ORidBagBinary):
                        itr = iter(data.entries)
                        while True:
                            try:
                                rid_tuple = itr.__next__()
                                edge_list.append(instance)
                                instance.setRID(rid_tuple[0], rid_tuple[1])
                                instance = self.getinstance(class_name)
                            except StopIteration:
                                break
                    return edge_list

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
        super().__init__()
        self.__codec = OCodec()
        self.__codec.serialization_decoder = self.decode
        self.__codec.serialization_encoder = self.encode
        self.__codec.toobject = self.toobject
        self.class_name = None

    def encode(self, data:BaseVertex):
        if data:
            result_head = b''
            logging.debug("start binary serializing data: {}".format(data))

            # write version
            result_head += self.__codec.writebyte(0)

            # write class name
            class_name = data.__class__.__name__
            result_head += self.__codec.writevarintstring(class_name)

            result_values = b''

            fields = data.persistent_attributes()
            temp_header_bytes = dict()
            temp_value_bytes = dict()
            temp_type_bytes = dict()
            byte_count = len(result_head)

            for field in fields:
                if hasattr(data, field):
                    value = getattr(data, field)
                    # check if the field is another vertex
                    if value:
                        try:
                            temp_header_bytes[field] = (self.__codec.writevarintstring(field))
                            type = self.__codec.findotype(value)
                            temp_type_bytes[field] = (self.__codec.writeotype(type))

                            # byte length of key string, type and position
                            byte_count += len(temp_header_bytes[field]) + len(temp_type_bytes[field]) + 4

                            temp_value_bytes[field] = (self.__codec.writevalue(type, value))

                        except TypeNotFoundException as err:
                            logging.error(err)

                else:
                    logging.info("class '{}' has no attribute with name '{}'".format(class_name, field))

            # concat the bytes to fit the right order
            for field in fields:
                if field in temp_header_bytes:
                    result_head += temp_header_bytes[field]
                    # position of the data
                    result_head += self.__codec.writeint(byte_count)
                    result_head += temp_type_bytes[field]

                    result_values += temp_value_bytes[field]
                    byte_count += len(temp_value_bytes[field])


        return result_head + result_values

    def decode(self, data):
        try:
            if len(data) != 0:
                logging.debug("start binary deserializing bytes: {}".format(data))

                # start deserializing
                # first read byte
                version, rest = self.__codec.readbyte(data)

                # read class name
                class_name, rest = self.__codec.readvarintstring(rest)
                position_delta = 0

                record = dict()

                first_run = True
                first_pos = None

                # read fields and pointers
                while True:
                    if first_pos and self.__codec.position >= first_pos:
                        break

                    length, rest = self.__codec.readvarint(rest)
                    if length == 0:
                        break

                    if length > 0:
                        field_name, rest = self.__codec.readbytes(length, rest)
                        pos, rest = self.__codec.readint(rest)
                        type, rest = self.__codec.readbyte(rest)

                        if first_run:
                            first_pos = pos
                            first_run = False

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
            return record, class_name, rest[first_pos:]
        except Exception as err:
            logging.error(err)


class OCSVSerializer(OSerializer):
    def __init__(self):
        super().__init__()
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
                        data_dict = parser.serialization_decoder(ORidBagDocument(ORidBagType.EMBEEDED), base64_binary)

                        logging.debug("decoded base64: " + str(base64_binary))

                        # map ids to instance
                        instance.linkdict[field_name] = data_dict['links']

                        logging.debug("parsed data from base64: {}".format(data_dict))


                else:
                    raise SerializationException("could not split '{}' by :".format(field))

            return instance
        else:
            raise SerializationException("could not split record content '{}' by @".format(decoded_str))

