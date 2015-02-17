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

from client.o_db_base import BaseVertex, BaseEdge, BaseEntity
from client.o_db_set import Select, Class, QueryType, Vertex, Edge
from common.o_db_exceptions import OPyClientException, SerializationException
from database.o_db_codec import OCodec
from database.o_db_connection import OConnection
from database.o_db_constants import ODBType, OModeChar, OCommandClass, ORidBagType, OSerialization
from database.o_db_driverconfig import ODriverConfig
from database.o_db_serializer import OCSVSerializer, OBinarySerializer
from common.o_db_model import ORidBagDocument, OSQLCommand, ORidBagBinary
from database.o_db_ops import ODB


__author__ = 'daill'

class OClient(object):
    # defines the base class
    baseclass = BaseEntity
    entities = dict()

    """
    This object has to be implemented by all object which should be auto saved and unfolded.
    It can be used to create custom class derivations of vertex class V and edge class E. It should be used
    to save vertices and edges as well as deleting them.
    """
    def __init__(self, database:str, user_name:str, user_password:str, host:str=None, port:int=None):
        try:
            # create the db object
            self.__odb = ODB()

            # create a connection object
            self.__connection = OConnection(host, port)

            # connect to db
            self.__odb.connect(self.__connection, user_name=user_name, user_password=user_password)

            # open the given database
            self.__odb.dbopen(self.__connection, database_name=database, database_type=ODBType.GRAPH.value, user_name="root", user_password="root")

        except Exception as err:
            logging.error(err)
            raise OPyClientException(err)

        # object cache rid -> object
        self.cache = dict()

        # trigger dict creation process
        self.createentitydict(OClient.baseclass)

        pass

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


    def add_record(self):
        pass

    def save(self, obj:object):
        """
        Process the given object to store the entities into the database
        :param obj:
        :return: True if the save has been completed, otherwise False
        """
        try:
            if isinstance(obj, BaseVertex):
                print("jap")
                data_to_store = obj.persistent_attributes()

                for attr_name in data_to_store:
                    print("attr " + attr_name)
                    print("attr_content " + obj.__getattribute__(attr_name))
        except AttributeError as e:
            print(e)


    def add_vertex(self, p_object:BaseVertex):
        pass

    def del_vertex(self):
        """
        Deletes a vertex
        """
        pass

    def createedge(self, object:BaseEdge):
        """
        Creates a simple edge between two vertices.
        """
        try:
            self.create(Edge(object))
        except Exception as err:
            logging.error(err)

    def createvertex(self, object:BaseVertex):
        """
        Wrapper method to iterate through the whole object tree and add it to the database
        """
        try:
            # add object to database to get a rid
            object = self.create(Vertex(object))

            # get all outgoing edges
            edges = object.getoutedges()

            # check if there are edges and iterate through them
            if edges:
                for key in edges:
                    edge = edges[key]
                    out_vertex = edge.out_vertex

                    # recursive call
                    self.createvertex(out_vertex)

                    #  outvertex should now own a rid, so we can persist an edge
                    edge = self.create(Edge(edge))

        except Exception as err:
            logging.error(err)


    def create(self, query_type:QueryType):
        if isinstance(query_type, Vertex):
            result_query = query_type.parse()
            persistent_object = query_type.getobject()

            # execute command
            command = OSQLCommand(result_query, non_text_limit=-1, fetchplan="", serialized_params="")
            response_data =  self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)

            # steps to extract data from response
            # TODO: sync vs. asynch
            if "success_status" in response_data:
                status = response_data.get("success_status")
                if status == 0:
                    # next to an exception there are various reasons for success or a failure, so we
                    # we have to check the status
                    if "result" in response_data:
                        logging.debug("parse synch response result for adding vertex")
                        result_data = response_data.get("result")
                        for records_data in result_data:
                            if "records" in records_data:
                                for record in records_data.get("records"):
                                    if "cluster-id" in record and "cluster-position" in record:
                                        persistent_object.cluster = record.get("cluster-id")
                                        persistent_object.position = record.get("cluster-position")
                                        persistent_object.version = record.get("record-version")
                                    else:
                                        logging.error("no cluster information available")
                                        # possibly raise an exception
                    if "asynch-result-type" in response_data:
                        logging.info("cannot handle asynch response, yet")
            else:
                logging.info("adding vertex was not successful")

            return persistent_object
        elif isinstance(query_type, Class):
            try:
                query_string = query_type.parse()

                # fetchplan is only needed on select query
                command = OSQLCommand(query_string, non_text_limit=-1, fetchplan="", serialized_params="")
                return self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
            except Exception as err:
                logging.error(err)
        elif isinstance(query_type, Edge):
            try:
                result_query = query_type.parse()
                persistent_object = query_type.getobject()

                # execute command
                command = OSQLCommand(result_query, non_text_limit=-1, fetchplan="", serialized_params="")
                response_data =  self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
                logging.debug("response data '{}'".format(response_data))

                # steps to extract data from response
                # TODO: sync vs. asynch
                if response_data:
                    if "success_status" in response_data:
                        status = response_data.get("success_status")
                        if status == 0:
                            # next to an exception there are various reasons for success or a failure, so we
                            # we have to check the status
                            if "result" in response_data:
                                logging.debug("parse synch response result for adding edge")
                                result_data = response_data.get("result")
                                for records_data in result_data:
                                    if "records" in records_data:
                                        for record in records_data.get("records"):
                                            if "cluster-id" in record and "cluster-position" in record:
                                                persistent_object.cluster = record.get("cluster-id")
                                                persistent_object.position = record.get("cluster-position")
                                                persistent_object.version = record.get("record-version")
                                            else:
                                                logging.error("no cluster information available")
                                                # possibly raise an exception
                            if "asynch-result-type" in response_data:
                                logging.info("cannot handle asynch response, yet")
            except Exception as err:
                logging.error(err)


    def add_edge(self):
        """
        Creates an edge of the given type from one vertex to another
        """
        pass

    def del_edge(self):
        """
        Deletes an edge between two vertices
        """
        pass

    def fetch(self, query:QueryType):
        try:
            if isinstance(query, Select):
                query_string = query.parse()
                # fetchplan is only needed on select query
                command = OSQLCommand(query_string, non_text_limit=-1, fetchplan="*:-1", serialized_params="")
                result_data = self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.IDEMPOTENT, command_payload=command)

                logging.debug("select received {}".format(result_data))

                clazz = query.getclass()

                # resulting objects list
                fetchedobjects = dict()
                returningobject = None
                firstrun = True

                if issubclass(clazz, BaseEntity):
                    if "success_status" in result_data:
                        status = result_data.get("success_status")
                        if status == 0:
                            # next to an exception there are various reasons for success or a failure, so we
                            # we have to check the status
                            if "result" in result_data:
                                logging.debug("parse sync response result for fetching vertex")
                                result_data = result_data.get("result")
                                for records_data in result_data:
                                    if "records" in records_data:
                                        for record in records_data.get("records"):
                                            if "cluster-id" in record and "cluster-position" in record and "record-content" in record:
                                                # create objects and add values
                                                # add to cache
                                                try:
                                                    clusterid = record.get("cluster-id")
                                                    clusterposition = record.get("cluster-position")
                                                    version = record.get("record-version")

                                                    parsedobject = self.parse_object(record_content=record.get("record-content"), clazz=clazz)

                                                    # parsedobject.setRID(clusterid, clusterposition)
                                                    # parsedobject.version = version
                                                    #
                                                    # if firstrun and isinstance(parsedobject, clazz):
                                                    #     firstrun = False
                                                    #     returningobject = parsedobject
                                                    #
                                                    # fetchedobjects[parsedobject.getRID()] = parsedobject
                                                    #
                                                    # logging.debug('{} {} {} {}'.format(clusterid, clusterposition, version, record.get("record-content")))

                                                except SerializationException as err:
                                                        logging.error(err)

                                            else:
                                                logging.error("no cluster information available")
                                                # possibly raise an exception


                            if "asynch-result-type" in result_data:
                                logging.info("cannot handle asynch response, yet")
                    else:
                        logging.info("no data fetched")

                return result_data
            else:
                return None
        except Exception as err:
            logging.error(err)

    def close(self):
        """
        Close connection
        """
        self.__odb.dbclose(self.__connection)

    def parse_object(self, record_content:str, clazz):
        logging.debug("start parsing record content")

        if issubclass(clazz, BaseVertex):
            logging.debug("class is from type BaseVertex")
        elif issubclass(clazz, BaseEdge):
            logging.debug("class is from type BaseEdge")
        else:
            raise SerializationException("could not determine type of given class to parse")

        if record_content:
            if ODriverConfig.SERIALIZATION == OSerialization.SERIALIZATION_CSV:
                serializer = OCSVSerializer()
            else:
                serializer = OBinarySerializer()
            serializer.entities = self.entities

            # get the deserialized data
            data, class_name, rest = serializer.decode(record_content)

            if not class_name:
                class_name = clazz.__name__

            parsedobject = serializer.toobject(class_name, data)

            return parsedobject
        else:
            raise SerializationException("record content string is empty")


    # build entity dict
    def createentitydict(self, base_class):
        subclasses = base_class.__subclasses__()
        for clazz in subclasses:
            OClient.entities[clazz.__name__] = clazz.__module__
            self.createentitydict(clazz)