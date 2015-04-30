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

from opy.client.o_db_base import BaseVertex, BaseEdge, BaseEntity, SystemType, OSchema
from opy.client.o_db_set import Select, Class, QueryType, Vertex, Edge, Update, Create, Drop, GraphType, Vertices, \
    Edges, Property, Delete, Move, Traverse, Truncate
from opy.common.o_db_exceptions import OPyClientException, SerializationException
from opy.database.o_db_connection import OConnection
from opy.common.o_db_constants import ODBType, OModeChar, OCommandClass, OSerialization
from opy.database.o_db_driverconfig import ODriverConfig
from opy.database.o_db_serializer import OCSVSerializer, OBinarySerializer
from opy.common.o_db_model import OSQLCommand, ORidBagBinary
from opy.database.o_db_ops import ODB


__author__ = 'daill'

class OClient(object):
    # defines the base class
    baseclass = BaseEntity
    entities = dict()
    schema = None

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

        #read schema
        self.readschema()

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

    def delete(self, query:GraphType):
        try:
            if isinstance(query, Update):
                query_string = query.parse()
                # fetchplan is only needed on select query
                command = OSQLCommand(query_string, non_text_limit=-1, fetchplan=query.fetchplan, serialized_params="")
                result_data = self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.IDEMPOTENT, command_payload=command)

                logging.debug("select received {}".format(result_data))

                return result_data
        except Exception as err:
            logging.error(err)

    def update(self, query:GraphType):
        try:
            if isinstance(query, Update):
                query_string = query.parse()
                # fetchplan is only needed on select query
                command = OSQLCommand(query_string, non_text_limit=-1, fetchplan=query.fetchplan, serialized_params="")
                result_data = self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.IDEMPOTENT, command_payload=command)

                logging.debug("select received {}".format(result_data))

                return result_data
        except Exception as err:
            logging.error(err)

    def createvertex(self, object:Vertex):
        """
        Wrapper method to iterate through the whole object tree and add it to the database
        """
        try:
            # add object to database to get a rid
            object = self.create(object)

            # get all outgoing edges
            edges = object.getoutedges()

            # check if there are edges and iterate through them
            if edges:
                for key in edges:
                    edge_list = edges[key]
                    for edge in edge_list:

                        out_vertex = edge.out_vertex

                        # recursive call
                        self.createvertex(Vertex(out_vertex))

                        # outvertex should now own a rid, so we can persist an edge
                        edge = self.create(Edge(edge))

            return object
        except Exception as err:
            logging.error(err)

    def createedge(self, object:Edge):
        """
        Creates a simple edge between two vertices.
        """
        try:
            self.create(object)

            return object
        except Exception as err:
            logging.error(err)

    def exec(self, query:str, fetchplan:str, raw:bool=True):
        try:
            # fetchplan is only needed on select query
            command = OSQLCommand(query, non_text_limit=-1, fetchplan=fetchplan, serialized_params="")
            response_data = self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)

            if raw:
                return response_data
            # else:
                # steps to extract data from response
                # TODO: sync vs. asynch
                # if response_data:
                #     if "success_status" in response_data:
                #         status = response_data.get("success_status")
                #         if status == 0:
                #             # next to an exception there are various reasons for success or a failure, so we
                #             # we have to check the status
                #             if "result" in response_data:
                #                 logging.debug("parse synch response result for adding edge")
                #                 result_data = response_data.get("result")
                #                 for records_data in result_data:
                #                     if "records" in records_data:
                #                         for record in records_data.get("records"):
                #                             if "cluster-id" in record and "cluster-position" in record:
                #                                 persistent_object.setRID(record.get("cluster-id"), record.get("cluster-position"))
                #                                 persistent_object.version = record.get("record-version")
                #                             else:
                #                                 logging.error("no cluster information available")
                #                                 # possibly raise an exception
                #             if "asynch-result-type" in response_data:
                #                 logging.info("cannot handle asynch response, yet")

        except Exception as err:
            logging.error(err)


    def delete(self, query_type:QueryType):
        try:
            query_string = query_type.parse()

            # fetchplan is only needed on select query
            command = OSQLCommand(query_string, non_text_limit=-1, fetchplan='', serialized_params="")
            return self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
        except Exception as err:
            logging.error(err)

    def truncate(self, query_type:QueryType):
        try:
            query_string = query_type.parse()

            # fetchplan is only needed on select query
            command = OSQLCommand(query_string, non_text_limit=-1, fetchplan='', serialized_params="")
            return self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
        except Exception as err:
            logging.error(err)

    def drop(self, query_type:GraphType):
        try:
            query_string = query_type.parse()

            # fetchplan is only needed on select query
            command = OSQLCommand(query_string, non_text_limit=-1, fetchplan=query_type.fetchplan, serialized_params="")
            return self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
        except Exception as err:
            logging.error(err)

    def move(self, query_type:GraphType):
        try:
            query_string = query_type.parse()

            # fetchplan is only needed on select query
            command = OSQLCommand(query_string, non_text_limit=-1, fetchplan=query_type.fetchplan, serialized_params="")
            return self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
        except Exception as err:
            logging.error(err)

    def do(self, query_action:QueryType):
        if not query_action:
            raise OPyClientException("you have to specify a query")

        if isinstance(query_action, Select):
            return self.fetch(query_action)
        elif isinstance(query_action, Traverse):
            return self.fetch(query_action)
        elif isinstance(query_action, Update):
            return self.update(query_action)
        elif isinstance(query_action, Delete):
            return self.delete(query_action)
        elif isinstance(query_action, Truncate):
            return self.truncate(query_action)
        elif isinstance(query_action, Drop):
            return self.drop(query_action)
        elif isinstance(query_action, Move):
            return self.move(query_action)
        elif isinstance(query_action, Create):
            type = query_action.type
            if isinstance(type, Vertex):
                return self.createvertex(type)
            elif isinstance(type, Vertices):
                object = type.getobject()
                if isinstance(object, list) or isinstance(object, set):
                    for vertex in object:
                        if isinstance(vertex, BaseVertex):
                            self.createvertex(Vertex(vertex))
                        else:
                            logging.error("element has to subclass BaseVertex")
                elif isinstance(object, dict):
                    for vertex in object.items():
                        if isinstance(vertex, BaseVertex):
                            self.createvertex(Vertex(vertex))
                        else:
                            logging.error("element has to subclass BaseVertex")
                return type.getobject()
            elif isinstance(type, Edge):
                return self.createedge(type)
            elif isinstance(type, Edges):
                object = type.getobject()
                if isinstance(object, list) or isinstance(object, set):
                    for edge in object:
                        if isinstance(edge, BaseEdge):
                            self.createedge(Edge(edge))
                        else:
                            logging.error("element has to subclass BaseEdge")
                elif isinstance(object, dict):
                    for edge in object.items():
                        if isinstance(edge, BaseEdge):
                            self.createedge(Edge(edge))
                        else:
                            logging.error("element has to subclass BaseEdge")
                return type.getobject()
            elif isinstance(type, Class):
                return self.create(type)
            elif isinstance(type, Property):
                return self.create(type)
            elif isinstance(type, str):
                return
            else:
                raise OPyClientException("don't know how to handle type '{}'".format(str(type)))
        else:
            raise OPyClientException("i don't know what to do")


    def create(self, query_type:GraphType):
        if isinstance(query_type, Vertex) or isinstance(query_type, Vertices):
            result_query = query_type.parse()
            persistent_object = query_type.getobject()

            # execute command
            command = OSQLCommand(result_query, non_text_limit=-1, fetchplan=query_type.fetchplan, serialized_params="")
            response_data = self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
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
                            logging.debug("parse synch response result for adding vertex")
                            result_data = response_data.get("result")
                            for records_data in result_data:
                                if "records" in records_data:
                                    for record in records_data.get("records"):
                                        if "cluster-id" in record and "cluster-position" in record:
                                            persistent_object.setRID(record.get("cluster-id"), record.get("cluster-position"))
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
                command = OSQLCommand(query_string, non_text_limit=-1, fetchplan=query_type.fetchplan, serialized_params="")
                return self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
            except Exception as err:
                logging.error(err)
        elif isinstance(query_type, Edge):
            try:
                result_query = query_type.parse()
                persistent_object = query_type.getobject()

                # execute command
                command = OSQLCommand(result_query, non_text_limit=-1, fetchplan=query_type.fetchplan, serialized_params="")
                response_data = self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
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
                                                persistent_object.setRID(record.get("cluster-id"), record.get("cluster-position"))
                                                persistent_object.version = record.get("record-version")
                                            else:
                                                logging.error("no cluster information available")
                                                # possibly raise an exception
                            if "asynch-result-type" in response_data:
                                logging.info("cannot handle asynch response, yet")
            except Exception as err:
                logging.error(err)
        elif isinstance(query_type, Property):
            try:
                result_query = query_type.parse()

                # execute command
                command = OSQLCommand(result_query, non_text_limit=-1, fetchplan=query_type.fetchplan, serialized_params="")
                response_data = self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command)
                logging.debug("response data '{}'".format(response_data))

                return response_data
            except Exception as err:
                logging.error(err)

    def fetch(self, query_type:QueryType):
        try:
            if isinstance(query_type, Select) or isinstance(query_type, Traverse):
                query_string = query_type.parse()
                # fetchplan is only needed on select query
                command = OSQLCommand(query_string, non_text_limit=-1, fetchplan=query_type.fetchplan, serialized_params="")
                result_data = self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.IDEMPOTENT, command_payload=command)

                logging.debug("{} received {}".format(query_type.__class__.__name__,result_data))

                clazz = query_type.getclass()

                # resulting objects list
                fetchedobjects = dict()
                returningobject = dict()
                resultdata = None

                # if issubclass(clazz, BaseEntity):
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

                                                if '#{}:{}'.format(clusterid, clusterposition) not in returningobject:

                                                    parsedobject, resultdata = self.parseobject(record_content=record.get("record-content"), clazz=clazz)


                                                    if not isinstance(parsedobject, dict):
                                                        parsedobject.setRID(clusterid, clusterposition)
                                                        parsedobject.version = version

                                                        # if isinstance(parsedobject, clazz):
                                                        #     returningobject[parsedobject.getRID()] = parsedobject

                                                        fetchedobjects[parsedobject.getRID()] = parsedobject
                                                    else:
                                                        rid = "#{}:{}".format(clusterid, clusterposition)
                                                        fetchedobjects[rid] = parsedobject

                                                    logging.debug("clusterid '{}' clusterposition '{}'  version '{}'  content '{}'".format(clusterid, clusterposition, version, record.get("record-content")))

                                            except SerializationException as err:
                                                    logging.error(err)

                                        else:
                                            logging.error("no cluster information available")
                                            # possibly raise an exception
                        if "asynch-result-type" in result_data:
                            logging.info("cannot handle asynch response, yet")
                else:
                    logging.info("no data fetched")

                # now set the correct references based of the fetched objects
                for rid in fetchedobjects:
                    # note that only embedded document don't own a RID therefore we only need to check the edges
                    object = fetchedobjects[rid]

                    if isinstance(object, BaseVertex):
                        edge_dict = object.out_edges

                        for edge_dict_key in edge_dict:
                            for obj in edge_dict[edge_dict_key]:
                                if isinstance(obj, list):
                                    for edge in obj:
                                        edge_rid = '#{}:{}'.format(edge.tmp_rid[0], edge.tmp_rid[1])
                                        if edge_rid in fetchedobjects:
                                            edge.out_vertex = fetchedobjects[edge_rid]
                                elif isinstance(obj, BaseEdge):
                                    edge_rid = '#{}:{}'.format(obj.tmp_rid[0], obj.tmp_rid[1])
                                    if edge_rid in fetchedobjects:
                                        obj.out_vertex = fetchedobjects[edge_rid]

                        edge_dict = object.in_edges

                        for edge_dict_key in edge_dict:
                            for obj in edge_dict[edge_dict_key]:
                                if isinstance(obj,list):
                                    for edge in obj:
                                        edge_rid = '#{}:{}'.format(edge.tmp_rid[0], edge.tmp_rid[1])
                                        if edge_rid in fetchedobjects:
                                            edge.in_vertex = fetchedobjects[edge_rid]
                                elif isinstance(obj, BaseEdge):
                                    edge_rid = '#{}:{}'.format(obj.tmp_rid[0], obj.tmp_rid[1])
                                    if edge_rid in fetchedobjects:
                                        obj.in_vertex = fetchedobjects[edge_rid]
                    elif isinstance(object, BaseEdge):
                        if isinstance(object.tmp_rid, dict):
                            if 'in' in object.tmp_rid:
                                in_rid = '#{}:{}'.format(object.tmp_rid['in'][0], object.tmp_rid['in'][1])
                                if in_rid in fetchedobjects:
                                    object.in_vertex = fetchedobjects[in_rid]
                            if 'out' in object.tmp_rid:
                                out_rid = '#{}:{}'.format(object.tmp_rid['out'][0], object.tmp_rid['out'][1])
                                if out_rid in fetchedobjects:
                                    object.out_vertex = fetchedobjects[out_rid]


                fetchedobjects["rest"] = resultdata
                return fetchedobjects
            else:
                return None
        except Exception as err:
            logging.error(err)

    def close(self):
        """
        Close connection
        """
        self.__odb.dbclose(self.__connection)

    def parseobject(self, record_content:str, clazz):
        logging.debug("start parsing record content")

        # if issubclass(clazz, BaseVertex):
        #     logging.debug("class is from type BaseVertex")
        # elif issubclass(clazz, BaseEdge):
        #     logging.debug("class is from type BaseEdge")
        # else:
        #     raise SerializationException("could not determine type of given class to parse")

        if record_content:
            if ODriverConfig.SERIALIZATION == OSerialization.SERIALIZATION_CSV:
                serializer = OCSVSerializer()
            else:
                serializer = OBinarySerializer()
            serializer.entities = self.entities
            serializer.schema = self.schema

            # get the deserialized data
            data, class_name, rest = serializer.decode(record_content)

            if not class_name:
                class_name = self.retrieveclassname(clazz)

            if class_name:
                parsedobject, result_data = serializer.toobject(class_name, data)
            else:
                parsedobject = data
                result_data = rest

            return parsedobject, result_data
        else:
            raise SerializationException("record content string is empty")

    def retrieveclassname(self, base_class):
        """
        This method decides whether it should use the default classname via magic member or custom method

        :param base_class:
        :return: class name for query
        """
        if base_class:
            if isinstance(base_class, SystemType):
                return base_class.getcustomclassname()
            return base_class.__name__
        return None


    def readschema(self, force:bool=False):

        try:
            if not OClient.schema or force:
                result = self.fetch(Select(OSchema, ['globalProperties'],()))
            OClient.schema = result['#-2:0']
        except Exception as err:
            logging.error(err)



    def createentitydict(self, base_class):
        """
        Create dictionary of domain classes used in the database
        :param base_class:
        :return:
        """
        subclasses = base_class.__subclasses__()
        for clazz in subclasses:
            OClient.entities[self.retrieveclassname(clazz)] = clazz.__module__
            self.createentitydict(clazz)