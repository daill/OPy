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
from inspect import isclass

import io
import logging
from client.o_db_base import BaseEntity, BaseEdge, BaseVertex
from client.o_db_utils import escapeclassname
from common.o_db_constants import OBinaryType, OPlainClass, OSQLOperationType, OSQLIndexType
from common.o_db_exceptions import SQLCommandException, WrongTypeException, OPyClientException

__author__ = 'daill'

class QueryType(object):
    def parse(self):
        raise NotImplementedError("You have to implement the exec method")

    def getclass(self):
        raise NotImplementedError("You have to implement the exec method")

class QueryElement(object):
    def __init__(self):
        self._query = ""

    def __str__(self):
        return self._query

class GraphType(QueryType):
    def __init__(self):
        self.fetchplan = ""
        self.operationtype = None

class WhereType(QueryElement):
    def __init__(self):
        super().__init__()

class Delete(QueryType):
    """
    Class to provide deletion of vertices and edges

    DELETE EDGE <rid>|FROM <rid>|TO <rid>|[<class>] [WHERE <conditions>]> [LIMIT <MaxRecords>]
    DELETE VERTEX <rid>|[<class>] [WHERE <conditions>]> [LIMIT <MaxRecords>]

    """
    def __init__(self, entity, *elements:QueryElement):
        self.__query_rule_index = ["Where", "Limit"]
        self.__elements = elements
        self.__entity = entity
        self.__entity.operationtype = OSQLOperationType.DELETE
        self.__fromrid = None
        self.__torid = None
        self.__rid = None

    def fromRID(self, obj):
        if isinstance(obj, str):
            self.__fromrid = obj
        elif isinstance(obj, BaseEntity):
            self.__fromrid =  obj.getRID()
        return self

    def toRID(self, obj):
        if isinstance(obj, str):
            self.__torid = obj
        elif isinstance(obj, BaseEntity):
            self.__torid =  obj.getRID()
        return self

    def byRID(self, rid:str=None):
        if rid:
            self.__rid = rid
        elif isinstance(self.__entity, BaseEntity):
            self.__rid =  self.__entity.getobject().getRID()
        return self


    def parse(self):
        try:
            # inner method for building the query string based on the given information
            # it's the same action for edges as well as vertices
            def _decidemethod(name:str=None):
                try:
                    query_string = io.StringIO()
                    if self.__rid:
                        return " {} ".format(self.__rid)
                    elif self.__fromrid or self.__torid:
                        if self.__fromrid:
                            query_string.write(" from ")
                            query_string.write(self.__fromrid)
                        if self.__torid:
                            query_string.write(" to ")
                            query_string.write(self.__torid)
                        query_string.write(" ")
                        result = query_string.getvalue()
                        return result
                    else:
                        return " {} ".format(name)
                except Exception as err:
                    logging.error(err)
                finally:
                    query_string.close()

            # inner method to build the query string based on the given Where and Limit clause
            def _parseelements():
                try:
                    query_string = io.StringIO()
                    for element in self.__elements:
                        self.__query_dict[element.__class__.__name__] = str(element)

                    for key in self.__query_rule_index:
                        if key in self.__query_dict:
                            query_string.write(" ")
                            query_string.write(self.__query_dict[key])
                    result = query_string.getvalue()

                    return result
                except Exception as err:
                    logging.error(err)
                finally:
                    query_string.close()

            query_string = io.StringIO()
            self.__query_dict = dict()

            if isclass(self.__entity):
                if issubclass(self.__entity, GraphType):
                    if issubclass(self.__entity, Vertex):
                        # make sure from and to are None
                        if self.__fromrid or self.__torid:
                            logging.warning("it's not allowed to use from and/or to on vertices")
                            self.__fromrid = None
                            self.__torid = None
                        query_string.write("delete vertex ")
                    elif issubclass(self.__entity, Edge):
                        query_string.write("delete edge ")
                    query_string.write(_decidemethod())

            elif isinstance(self.__entity, GraphType):
                query_string.write(self.__entity.parse())

                if not isinstance(self.__entity, Class):
                    if isinstance(self.__entity, Vertex):
                        # make sure from and to are None
                        if self.__fromrid or self.__torid:
                            logging.warning("it's not allowed to use from and/or to on vertices")
                            self.__fromrid = None
                            self.__torid = None

                    query_string.write(_decidemethod(self.__entity.getobject().__class__.__name__))

            elif isinstance(self.__entity, BaseEntity):
                query_string.write("delete ")

                # if there is an object use it and get the RID
                if isinstance(self.__entity, BaseVertex):
                    query_string.write(" vertex ")
                elif isinstance(self.__entity, BaseEdge):
                    query_string.write(" edge ")

                query_string.write(self.__entity.getRID())
                query_string.write(" ")

            query_string.write(_parseelements())

            result_string = query_string.getvalue()

            logging.debug("parsed sql string: '{}' from data '{}'".format(result_string, self.__query_dict))

            return result_string

        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()


class Create(QueryType):
    def __init__(self, type:GraphType):
        self.type = type
        self.type.operationtype = OSQLOperationType.CREATE

    def parse(self):
        return self.type.parse()

class Drop(QueryType):
    """
    DROP CLASS <class>
    """
    def __init__(self, type:GraphType):
        self.type = type
        self.type.operationtype = OSQLOperationType.DROP

    def parse(self):
        return self.type.parse()

class Property(GraphType):
    def __init__(self, persistent_class:BaseEntity, property_name:str, property_type:OBinaryType=None, linked_type:OBinaryType=None, linked_class:str=None):
        super().__init__()
        self.__persistent_class = persistent_class
        self.__class_name = getattr(persistent_class, '__name__')
        self.__property_name = property_name
        self.__property_type = property_type
        self.__linked_type = linked_type
        self.__linked_class = linked_class

    def parse(self):
        try:
            query_string = io.StringIO()
            if self.operationtype == OSQLOperationType.CREATE:
                query_string.write("create property ")
                query_string.write(self.__class_name)
                query_string.write(".")
                query_string.write(self.__property_name)
                query_string.write(" ")
                query_string.write(self.__property_type.name)

                if self.__linked_type and self.__linked_class:
                    raise OPyClientException("two concurrent parameters set")

                if self.__linked_type:
                    query_string.write(" ")
                    query_string.write(self.__linked_type.name)
                elif self.__linked_class:
                    query_string.write(" ")
                    query_string.write(self.__linked_class)
            elif self.operationtype == OSQLOperationType.DROP:
                query_string.write("drop property ")
                query_string.write(self.__class_name)
                query_string.write(".")
                query_string.write(self.__property_name)


            result_string = query_string.getvalue()

            return result_string
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()


    @classmethod
    def withlinkedclass(cls, persistent_class:BaseEntity, property_name:str, property_type:OBinaryType, linked_class:str=None):
        return cls(persistent_class, property_name, property_type, None, linked_class)

    @classmethod
    def withlinkedtype(cls, persistent_class:BaseEntity, property_name:str, property_type:OBinaryType, linked_type:OBinaryType):
        return cls(persistent_class, property_name, property_type, linked_type, None)

class Cluster(GraphType):
    def __init__(self):
        super().__init__()

class Class(GraphType):
    def __init__(self, persistent_class:BaseEntity, class_type:OPlainClass=None):
        super().__init__()
        self.__persistent_class = persistent_class
        self.__class_name = getattr(persistent_class, '__name__')

        if(class_type):
            self.__class_type = class_type.value
        # save module to database to identify on later following selection process
        self.__class_module = persistent_class.__module__

    def parse(self):
        try:
            query_string = io.StringIO()

            if self.operationtype == OSQLOperationType.CREATE:
                query_string.write("create class ") # start off with writing the common part of the command
                query_string.write(escapeclassname(self.__class_name)) # append class name
                query_string.write(" extends ")
                query_string.write(self.__class_type)
            elif self.operationtype == OSQLOperationType.DROP:
                query_string.write("drop class ")
                query_string.write(self.__class_name)
            elif self.operationtype == OSQLOperationType.DELETE:
                query_string.write("delete ")
                if issubclass(self.__persistent_class, BaseEdge):
                    query_string.write(" edge ")
                elif issubclass(self.__persistent_class, BaseVertex):
                    query_string.write(" vertex ")
                query_string.write(self.__class_name)

            result_query = query_string.getvalue()

            return result_query
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

class Edges(GraphType):
    def __init__(self, object):
        super().__init__()
        self.__object = object

    def getobject(self):
        return self.__object

class Edge(GraphType):
    """
    CREATE EDGE <class> [CLUSTER <cluster>] FROM <rid>|(<query>)|[<rid>]* TO <rid>|(<query>)|[<rid>]*
                    [SET <field> = <expression>[,]*]|CONTENT {<JSON>}
                    [RETRY <retry> [WAIT <pauseBetweenRetriesInMs]]

    TODO: implement properties, specific cluster, select
    """
    def __init__(self, object:BaseEdge=None):
        super().__init__()
        self.__object = object
        self.__class_name = self.__object.__class__.__name__

    def getobject(self):
        return self.__object

    def parse(self):
        result_query = None
        try:
            if isinstance(self.__object, BaseEdge):
                query_string = io.StringIO()

                if self.operationtype == OSQLOperationType.CREATE:

                    query_string.write("create edge ") # start off with writing the common part of the command
                    query_string.write(escapeclassname(self.__class_name)) # append class name
                    query_string.write(" from ")
                    query_string.write(self.__object.in_vertex.getrid())
                    query_string.write(" to ")
                    query_string.write(self.__object.out_vertex.getrid())
                elif self.operationtype == OSQLOperationType.DELETE:
                    query_string.write("delete edge ") # start off with writing the common part of the command

                result_query = query_string.getvalue()

                logging.debug("result query: {}".format(result_query))
            else:
                raise WrongTypeException("object must implement BaseEdge class")
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

        return result_query

class Vertices(GraphType):
    def __init__(self, object):
        super().__init__()
        self.__object = object

    def getobject(self):
        return self.__object

class Vertex(GraphType):
    """
    Creates a new vertex by using the given class
    """
    def __init__(self, object:BaseVertex=None):
        super().__init__()
        self.__object = object
        self.__class_name = object.__class__.__name__
        pass

    def getobject(self):
        return self.__object

    def parse(self):
        result_query = None
        try:

            if isinstance(self.__object, BaseVertex):
                query_string = io.StringIO()

                if self.operationtype == OSQLOperationType.CREATE:
                    query_string.write("create vertex ") # start off with writing the common part of the command
                    query_string.write(escapeclassname(self.__class_name)) # append class name

                    # read "marked" attributes
                    data_to_store = self.__object.persistentattributes()

                    count = len(data_to_store)
                    if count > 0:
                        query_string.write(" set ")
                        for i in range(count):
                            attr_name = data_to_store[i]

                            query_string.write(attr_name)
                            query_string.write(" = \"")
                            query_string.write(str(self.__object.__getattribute__(attr_name)))
                            query_string.write("\"")
                            if i < count-1:
                                query_string.write(" , ")
                elif self.operationtype == OSQLOperationType.DELETE:
                    query_string.write("delete vertex ") # start off with writing the common part of the command

                result_query = query_string.getvalue()

                logging.debug("result query: {}".format(result_query))
            else:
                raise WrongTypeException("object must implement BaseVertex class")
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

        return result_query

class Insert(QueryType):
    """
    Factory to build an insert statement out of the given data and class types

    INSERT INTO [class:]<class>|cluster:<cluster>|index:<index>
        [(<field>[,]*) VALUES (<expression>[,]*)[,]*]|
        [SET <field> = <expression>|<sub-command>[,]*]|
        [CONTENT {<JSON>}]|
        [FROM <query>]
    """
    def __init__(self, persistent_object:BaseVertex):
        self.__clazz_name = getattr(persistent_object.__class__,'__name__')
        self.__object = persistent_object

    def parse(self):
        try:
            query_string = io.StringIO()

            query_string.write("insert into ")
            query_string.write(escapeclassname(self.__clazz_name))
            query_string.write(" ")

            persistent_attributes = self.__object.persistentattributes()

            if len(persistent_attributes) > 0:
                query_string.write("( ")
                for attribute in persistent_attributes:
                    query_string.write(attribute)
                    if persistent_attributes.index(attribute) != len(persistent_attributes)-1:
                        query_string.write(", ")
                query_string.write(" )")

            query_string.write(" values ")

            query_string.write("(")
            if len(persistent_attributes) > 0:
                for attribute in persistent_attributes:
                    query_string.write("'")
                    query_string.write(str(self.__object.__getattribute__(attribute)))
                    query_string.write("'")
                    if persistent_attributes.index(attribute) != len(persistent_attributes)-1:
                        query_string.write(",")
            query_string.write(")")

            result_query = query_string.getvalue()

            return result_query
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

class Select(QueryType):
    """
    Select statement which can be used to easily create a complete query

    SELECT [<Projections>] [FROM <Target> [LET <Assignment>*]]
        [WHERE <Condition>*]
        [GROUP BY <Field>*]
        [ORDER BY <Fields>* [ASC|DESC] *]
        [SKIP <SkipRecords>]
        [LIMIT <MaxRecords>]
        [FETCHPLAN <FetchPlan>]
        [TIMEOUT <Timeout> [<STRATEGY>]
        [LOCK default|record]
        [PARALLEL]
    """
    def __init__(self, clazz:BaseEntity, props:list=None, *elements:QueryElement):
        self.__elements = elements
        self.__query_dict = dict()
        self.__clazz = clazz
        self.__clazz_name = getattr(clazz,'__name__')
        self.__prefix = None
        self.__query_rule_index = ["Let", "Where", "GroupBy", "OrderBy", "Skip", "Limit", "Fetchplan", "Timeout", "Lock", "Parallel"]
        self.__props = props

    def parse(self):
        try:
            query_string = io.StringIO()
            query_string.write("select ")
            if len(self.__props) > 0:
                for projection in self.__props:
                    query_string.write(projection)
                    query_string.write(" ")
            query_string.write("from ")
            query_string.write(escapeclassname(self.__clazz_name))
            if self.__prefix:
                query_string.write(" ")
                query_string.write(self.__prefix)

            for element in self.__elements:
                self.__query_dict[element.__class__.__name__] = str(element)
            for key in self.__query_rule_index:
                if key in self.__query_dict:
                    query_string.write(" ")
                    query_string.write(self.__query_dict[key])

            result_string = query_string.getvalue()

            logging.debug("parsed sql string: '{}' from data '{}'".format(result_string, self.__query_dict))

            return result_string
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

    def getclass(self):
        return self.__clazz

class Let(QueryElement):
    def __init__(self, name:str, assignment:str):
        self.__name = name
        self.__assignment = assignment

    @classmethod
    def byselect(cls, name:str, element:QueryElement):
        try:
            if not isinstance(element, Select):
                raise SQLCommandException("currently only select is supported in let statements")

            query_string = io.StringIO()
            query_string.write("( ")
            query_string.write(str(element))
            query_string.write(" ) ")
            result_query = query_string.getvalue()
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()


        return cls(name, result_query)

    @classmethod
    def byfield(cls, name:str, assignment:str):
        return cls(name, assignment)

    def __str__(self):
        try:
            query_string = io.StringIO()
            query_string.write(" let ")
            query_string.write(self.__name)
            query_string.write(" = ")
            query_string.write(self.__assignment)
            query_string.write(" ")
            result_string = query_string.getvalue()

            return result_string
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()


class Update(QueryType):
    """
    Defines the update statement

    UPDATE <class>|cluster:<cluster>|<recordID>
      [SET|INCREMENT|ADD|REMOVE|PUT <field-name> = <field-value>[,]*]|[CONTENT|MERGE <JSON>]
      [UPSERT]
      [RETURN <returning> [<returning-expression>]]
      [WHERE <conditions>]
      [LOCK default|record]
      [LIMIT <max-records>] [TIMEOUT <timeout>]
    """
    def __init__(self, object, updateaction, *elements:QueryElement):
        self.__query_rule_index = ["Upsert", "Return", "Where", "Lock", "Limit"]
        self.__updateaction = updateaction
        self.__elements = elements
        self.__object = object

    def parse(self):
        try:
            self.__query_dict = dict()
            query_string = io.StringIO()
            query_string.write("update ")
            if isinstance(self.__object, str):
                # its a rid
                query_string.write(self.__object)
            elif isinstance(self.__object, BaseEntity):
                query_string.write(self.__object.getRID())
            else:
                # its a class
                query_string.write(self.__object.__name__)
            query_string.write(" ")
            query_string.write(str(self.__updateaction))

            for element in self.__elements:
                self.__query_dict[element.__class__.__name__] = str(element)

            for key in self.__query_rule_index:
                if key in self.__query_dict:
                    query_string.write(" ")
                    query_string.write(self.__query_dict[key])

            result_string = query_string.getvalue()

            logging.debug("parsed sql string: '{}' from data '{}'".format(result_string, self.__query_dict))

            return result_string
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()


class Upsert(QueryElement):
    def __init__(self):
        super().__init__()
        self._query = " upsert "

class Lock(QueryElement):
    def __init__(self, type:str):
        super().__init__()
        self._query = " lock {} ".format(type)

    @classmethod
    def default(cls):
        return cls("default")

    @classmethod
    def record(cls):
        return cls("record")

class Limit(QueryElement):
    def __init__(self, count:int, timeout:int=None):
        super().__init__()

        if timeout:
            self._query = " limit {} timeout {} ".format(count, timeout)
        else:
            self._query = " limit {} ".format(count)

    @classmethod
    def withTimeout(cls, count:int, timeout:int):
        return cls(count, timeout)

class Return(QueryElement):
    """
    RETURN: If a field has been specified, then only this field will be returned
    otherwise the whole record will be returned
    """
    def __init__(self, type:str, field:str=None):
        super().__init__()
        if field:
            self._query = " return {} {}".format(type, field)
        else:
            self._query = " return {}".format(type)

    @classmethod
    def count(cls):
        return cls("count")

    @classmethod
    def after(cls, field:str):
        return cls("after", field)

    @classmethod
    def before(cls, field:str):
        return cls("before", field)

class Lock(QueryElement):
    def __init__(self):
        pass

class From(QueryElement):
    def __init__(self, rid:str):
        self.rid = rid

class To(QueryElement):
    def __init__(self, rid:str):
        self.rid = rid


class UpdateActionElement(QueryElement):
    def __init__(self, type:str, fields:dict):
        self.__type = type
        self.__fields = fields

    def __str__(self):
        try:
            query_string = io.StringIO()
            query_string.write(" ")
            query_string.write(self.__type)
            query_string.write(" ")

            count = 1
            for field_name in self.__fields:
                query_string.write(field_name)
                query_string.write(" = ")
                if isinstance(self.__fields[field_name], str):
                    query_string.write("'")
                    query_string.write(str(self.__fields[field_name]))
                    query_string.write("'")
                else:
                    query_string.write(str(self.__fields[field_name]))

                if count < len(self.__fields):
                    query_string.write(", ")
                count += 1

            result_string = query_string.getvalue()

            return result_string
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

class Set(UpdateActionElement):
    def __init__(self, fields:dict):
        super().__init__("set", fields)

class Increment(UpdateActionElement):
    def __init__(self, fields:dict):
        super().__init__("increment", fields)

class Add(UpdateActionElement):
    def __init__(self, fields:dict):
        super().__init__("add", fields)

class Remove(UpdateActionElement):
    def __init__(self, fields:dict):
        super().__init__("remove", fields)

class Put(UpdateActionElement):
    def __init__(self, fields:dict):
        super().__init__("put", fields)

class Content(UpdateActionElement):
    def __init__(self):
        pass

class Merge(UpdateActionElement):
    def __init__(self):
        pass

class Condition(WhereType):
    def __init__(self, attribute_name:str):
        super().__init__()
        self.__attribute_name = attribute_name

    def isle(self, value:object):
        self._query = "{} <= {}".format(self.__attribute_name, self.__valuestring(value))
        return self

    def islt(self, value:object):
        self._query = "{} < {}".format(self.__attribute_name, self.__valuestring(value))
        return self

    def isge(self, value:object):
        self._query = "{} >= {}".format(self.__attribute_name, self.__valuestring(value))
        return self

    def isgt(self, value:object):
        self._query = "{} > {}".format(self.__attribute_name, self.__valuestring(value))
        return self

    def iseq(self, value:object):
        self._query = "{} = {}".format(self.__attribute_name, self.__valuestring(value))
        return self

    def isin(self, value:object):
        pass

    def __valuestring(self, value):
        if isinstance(value, str):
            return "'{}'".format(value)
        elif isinstance(value, int):
            return "{}".format(value)


class Where(QueryElement):
    def __init__(self, *objects):
        super().__init__()
        self.__elements = objects

    def __str__(self):
        try:
            query_string = io.StringIO()
            query_string.write(" where ")
            for element in self.__elements:
                if isinstance(element, Select):
                    query_string.write("(")
                    query_string.write(element.parse())
                    query_string.write(")")
                else:
                    query_string.write(str(element))
            query_string.write(" ")
            result_string = query_string.getvalue()

            return result_string
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()


class GroupBy(QueryElement):
    def __init__(self, grouping_field:str):
        self.__grouping_field = grouping_field

    def __str__(self):
        try:
            query_string = io.StringIO()
            query_string.write(" group by ")
            query_string.write(self.__grouping_field)
            query_string.write(" ")
            result_query = query_string.getvalue()

            return result_query
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

class OrderBy(QueryElement):
    def __init__(self, attribute_name:str, order:str="asc"):
        self.__attribute_name = attribute_name
        self.__order = order

    @classmethod
    def asc(cls, attribute_name:str):
        return cls(attribute_name)

    @classmethod
    def desc(cls, attribute_name:str):
        return cls(attribute_name, "desc")

    def __str__(self):
        try:
            query_string = io.StringIO()
            query_string.write(" order by ")
            query_string.write(self.__attribute_name)
            query_string.write(" ")
            query_string.write(self.__order)
            query_string.write(" ")
            result_string = query_string.getvalue()

            return result_string
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

class Or(WhereType):
    def __init__(self, *types:WhereType):
        super().__init__()
        try:
            query_string = io.StringIO()
            query_string.write(" ( ")
            for i,type in enumerate(types):

                query_string.write(str(type))
                query_string.write(" ")

                if i < len(types)-1:
                    query_string.write(" or ")

            query_string.write(" ) ")
            self._query = query_string.getvalue()
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

class And(WhereType):
    def __init__(self, *types:WhereType):
        super().__init__()
        try:
            query_string = io.StringIO()
            query_string.write(" ( ")
            for i,type in enumerate(types):

                query_string.write(str(type))
                query_string.write(" ")

                if i < len(types)-1:
                    query_string.write(" and ")

            query_string.write(" ) ")
            self._query = query_string.getvalue()
        except Exception as err:
            logging.error(err)

class Index(GraphType):
    """
    CREATE INDEX <name> [ON <class-name> (prop-names)] <type> [<key-type>] METADATA [{<json-metadata>}]
    If prop is type of LINKMAP or EMBEDDEDMAP you kann add "by key" or "by value" to the property name
    """
    def __init__(self, obj):
        super().__init__()
        self.__obj = obj
        self.__metadata = None
        self.__type = None
        self.__clazz = None
        self.__properties = None

    def on(self, clazz:BaseEntity, properties:list=None, type:OSQLIndexType=None):
        self.__clazz = clazz
        self.__properties = properties
        self.__type = type
        return self


    def withmeta(self, metadata:str):
        self.__metadata = metadata
        return self

    def parse(self):
        try:
            query_string = io.StringIO()


            if self.operationtype == OSQLOperationType.CREATE:
                query_string.write("create index ")
                if isinstance(self.__obj, str):
                    if self.__properties:
                        query_string.write(self.__obj)
                        query_string.write(" on ")
                        query_string.write(self.__clazz.__name__)
                        query_string.write(" (")
                        for i,prop in enumerate(self.__properties):
                            query_string.write(prop)
                            if i < len(self.__properties)-1:
                                query_string.write(", ")
                        query_string.write(") ")
                    else:
                        query_string.write(self.__clazz.__name__)
                        query_string.write(".")
                        query_string.write(self.__obj)

                if self.__type:
                    query_string.write(" ")
                    query_string.write(self.__type.value)

                if self.__metadata:
                    query_string.write(" metadata ")
                    query_string.write(self.__metadata)


            result_query = query_string.getvalue()

            return result_query
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()


