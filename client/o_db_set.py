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

import io
import logging
from client.o_db_base import BaseEntity, BaseEdge, BaseVertex
from client.o_db_utils import escapeclassname
from common.o_db_exceptions import SQLCommandException, WrongTypeException

__author__ = 'daill'

class QueryType(object):
    def __init__(self):
        pass

    def parse(self):
        raise NotImplementedError("You have to implement the exec method")

    def getclass(self):
        raise NotImplementedError("You have to implement the exec method")

class QueryElement(object):
    def __init__(self):
        self._query = ""

    def __str__(self):
        return self._query

class Linkage(QueryElement):
    def __init__(self):
        super().__init__()
        pass

class Delete(QueryType):
    def __init__(self):
        pass

class Create(QueryType):
    def __init__(self):
        pass

class Class(QueryType):
    def __init__(self, persistent_class:BaseEntity, class_type:str):
        self.__persistent_class = persistent_class
        self.__class_name = getattr(persistent_class, '__name__')
        self.__class_type = class_type
        # save module to database to identify on later following selection process
        self.__class_module = persistent_class.__module__

    def parse(self):
        query_string = io.StringIO()

        query_string.write("create class ") # start off with writing the common part of the command
        query_string.write(escapeclassname(self.__class_name)) # append class name
        query_string.write(" extends ")
        query_string.write(self.__class_type)

        result_query = query_string.getvalue()
        query_string.close()

        return result_query

class Edge(QueryType):
    """
    CREATE EDGE <class> [CLUSTER <cluster>] FROM <rid>|(<query>)|[<rid>]* TO <rid>|(<query>)|[<rid>]*
                    [SET <field> = <expression>[,]*]|CONTENT {<JSON>}
                    [RETRY <retry> [WAIT <pauseBetweenRetriesInMs]]

    TODO: implement properties, specific cluster, select
    """
    def __init__(self, object:BaseEdge):
        self.__object = object

    def getobject(self):
        return self.__object

    def parse(self):
        result_query = None
        try:
            if isinstance(self.__object, BaseEdge):
                query_string = io.StringIO()

                query_string.write("create edge ") # start off with writing the common part of the command
                query_string.write(escapeclassname(self.__object.__class__.__name__)) # append class name
                query_string.write(" from ")
                query_string.write(self.__object.in_vertex.getrid())
                query_string.write(" to ")
                query_string.write(self.__object.out_vertex.getrid())

                result_query = query_string.getvalue()

                logging.debug("result query: {}".format(result_query))
            else:
                raise WrongTypeException("object must implement BaseEdge class")
        except Exception as err:
            logging.error(err)
        finally:
            query_string.close()

        return result_query

class Vertex(QueryType):
    """
    Creates a new vertex by using the given class
    """
    def __init__(self, object:BaseVertex):
        self.__object = object

    def getobject(self):
        return self.__object

    def parse(self):
        result_query = None
        try:
            if isinstance(self.__object, BaseVertex):
                query_string = io.StringIO()

                query_string.write("create vertex ") # start off with writing the common part of the command
                query_string.write(escapeclassname(self.__object.__class__.__name__)) # append class name

                # read "marked" attributes
                data_to_store = self.__object.persistent_attributes()

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

                result_query = query_string.getvalue()

                logging.debug("result query: {}s".format(result_query))
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
        query_string = io.StringIO()

        query_string.write("insert into ")
        query_string.write(escapeclassname(self.__clazz_name))
        query_string.write(" ")

        persistent_attributes = self.__object.persistent_attributes()

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
        query_string.close()
        return result_query

class Property(QueryType):
    """
    TODO: implement to add properties to a class
    """
    def __init__(self):
        pass

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
    def __init__(self, clazz:BaseEntity, projections:list=None, *elements:QueryElement):
        self.__elements = elements
        self.__query_dict = dict()
        self.__clazz = clazz
        self.__clazz_name = getattr(clazz,'__name__')
        self.__prefix = None
        self.__query_rule_index = ["Let", "Where", "GroupBy", "OrderBy", "Skip", "Limit", "Fetchplan", "Timeout", "Lock", "Parallel"]
        self.__projections = projections

    def parse(self):
        query_string = io.StringIO()
        query_string.write("select ")
        if len(self.__projections) > 0:
            for projection in self.projections:
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

        query_string.close()

        return result_string

    def getclass(self):
        return self.__clazz

class Let(QueryElement):
    def __init__(self, name:str, assignment:str):
        self.__name = name
        self.__assignment = assignment

    @classmethod
    def byselect(cls, name:str, element:QueryElement):
        if not isinstance(element, Select):
            raise SQLCommandException("currently only select is supported in let statements")

        query_string = io.StringIO()
        query_string.write("( ")
        query_string.write(str(element))
        query_string.write(" ) ")
        result_query = query_string.getvalue()
        query_string.close()

        return cls(name, result_query)

    @classmethod
    def byfield(cls, name:str, assignment:str):
        return cls(name, assignment)

    def __str__(self):
        query_string = io.StringIO()
        query_string.write(" let ")
        query_string.write(self.__name)
        query_string.write(" = ")
        query_string.write(self.__assignment)
        query_string.write(" ")
        result_string = query_string.getvalue()
        query_string.close()
        return result_string

class Update(QueryType):
    def __init__(self, object:Class):
        pass

class Condition(QueryElement):
    def __init__(self, attribute_name:str):
        super().__init__()
        self.__attribute_name = attribute_name

    def isle(self, value:object):
        self._query = "{} <= '{}'".format(self.__attribute_name, value)
        return self

    def islt(self, value:object):
        self._query = "{} < '{}'".format(self.__attribute_name, value)
        return self

    def isge(self, value:object):
        self._query = "{} >= '{}'".format(self.__attribute_name, value)
        return self

    def isgt(self, value:object):
        self._query = "{} > '{}'".format(self.__attribute_name, value)
        return self

    def iseq(self, value:object):
        self._query = "{} = '{}'".format(self.__attribute_name, value)
        return self

    def isin(self, value:object):
        pass

class Where(QueryElement):
    def __init__(self, *linkages:Linkage):
        super().__init__()
        self.__linkage_elements = linkages

    def __str__(self):
        query_string = io.StringIO()
        query_string.write(" where ")
        for linkage in self.__linkage_elements:
            query_string.write(str(linkage))
        query_string.write(" ")
        result_string = query_string.getvalue()
        query_string.close()
        return result_string

class GroupBy(QueryElement):
    def __init__(self, grouping_field:str):
        self.__grouping_field = grouping_field

    def __str__(self):
        query_string = io.StringIO()
        query_string.write(" group by ")
        query_string.write(self.__grouping_field)
        query_string.write(" ")
        result_query = query_string.getvalue()
        query_string.close()
        return result_query

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
        query_string = io.StringIO()
        query_string.write(" order by ")
        query_string.write(self.__attribute_name)
        query_string.write(" ")
        query_string.write(self.__order)
        query_string.write(" ")
        result_string = query_string.getvalue()
        query_string.close()
        return result_string

class Or(Linkage):
    def __init__(self, condition_a:Condition, condition_b:Condition):
        super().__init__()
        self._query = "( {} or {} )".format(condition_a, condition_b)

class And(Linkage):
    def __init__(self, condition_a:Condition, condition_b:Condition):
        super().__init__()
        self._query = "( {} and {} )".format(condition_a, condition_b)

