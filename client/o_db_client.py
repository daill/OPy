import logging
from client.o_db_base import BaseVertex

from client.o_db_set import Select, Class, QueryType, Vertex, Edge
from common.o_db_exceptions import OPyClientException
from database.o_db_connection import OConnection
from database.o_db_constants import ODBType, OModeChar, OCommandClass
from database.protocol.o_op_request import OSQLCommand
from database.protocol.o_ops import ODB


__author__ = 'daill'

class OClient(object):
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
            self.__odb.dbopen(self.__connection, database_name=database, database_type=ODBType.DOCUMENT.value, user_name="root", user_password="root")
        except Exception as err:
            logging.error(err)
            raise OPyClientException(err)

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
            print(response_data)

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
                print(response_data)

                # steps to extract data from response
                # TODO: sync vs. asynch
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
                command = OSQLCommand(query_string, non_text_limit=-1, fetchplan="", serialized_params="")
                result_data = self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.IDEMPOTENT, command_payload=command)
                print('result' in result_data)

                # determine class
                # iterate through data
                # create instances
                #return instances
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