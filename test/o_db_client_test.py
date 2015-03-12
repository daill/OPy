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

import unittest
from client.o_db_base import BaseVertex
from client.o_db_set import Select, Class, Where, Condition, OrderBy, Let, QueryElement, GroupBy, Insert, Update, Set, \
    Upsert, Create, Vertex, Property, Delete, And, Or, Drop, Edge
from common.o_db_constants import OBinaryType
from test.model.o_db_test_model import TestObject, TestLocation, TestCoordinates, TestEdgeOne

__author__ = 'daill'

class ODBClientTests(unittest.TestCase):

    def test_selectwhere(self):
        query = Select(TestLocation, (), Where(Or(Condition("name").iseq("Eddies"),Condition("type").iseq("Pizaaria")))).parse()
        self.assertEqual(query, "select from TestLocation  where  ( name = 'Eddies'  or type = 'Pizaaria'  )  ")

        query = Select(TestLocation, (), Where(And(Condition("name").iseq("Eddies"),Condition("type").iseq("Pizaaria")))).parse()
        self.assertEqual(query, "select from TestLocation  where  ( name = 'Eddies'  and type = 'Pizaaria'  )  ")

        query = Select(TestLocation, (), Where(And(Or(Condition("test").iseq("1"), Condition("test").iseq("2")),Condition("type").iseq("Pizaaria")))).parse()
        self.assertEqual(query, "select from TestLocation  where  (  ( test = '1'  or test = '2'  )   and type = 'Pizaaria'  )  ")

        query = Select(TestLocation, (), Where(And(Or(Condition("test").iseq("1"), Condition("test").iseq("2"), Condition("test1").iseq("zz")),Condition("type").iseq("Pizaaria")))).parse()
        self.assertEqual(query, "select from TestLocation  where  (  ( test = '1'  or test = '2'  or test1 = 'zz'  )   and type = 'Pizaaria'  )  ")

        query = Select(TestLocation, (), Where(Select(TestLocation, (), Where(Condition("a").iseq("a"))))).parse()
        self.assertEqual(query, "select from TestLocation  where (select from TestLocation  where a = 'a' ) ")

    def test_selectorderofelements(self):
        query = Select(TestLocation,["l.a", "l.b"], OrderBy("a").asc(), Where(Condition("l.a").iseq("b"))).parse()
        self.assertEqual(query, "select l.a l.b from Location l where l.a = b;")

    def test_orderby(self):
        query = str(OrderBy("a"))
        self.assertEqual(query, " order by a asc ")
        query = str(OrderBy.asc("a"))
        self.assertEqual(query, " order by a asc ")
        query = str(OrderBy.desc("a"))
        self.assertEqual(query, " order by a desc ")

    def test_let(self):
        query = str(Let.byfield("$a", "address.city"))
        self.assertEqual(query, " let $a = address.city ")

    def test_groupby(self):
        query = str(GroupBy("name"))
        self.assertEqual(query, " group by name ")

    def test_create(self):
        query = Create(Vertex(TestLocation)).parse()


    def test_insert(self):
        location = TestCoordinates()
        location.lat = 5
        location.lng = '10'
        query = Insert(location).parse()
        self.assertEqual(query, "insert into TestLocation ( lat, lng ) values ('5','4')")

    def test_update(self):
        # query = Update(TestCoordinates, Set({'a':'5', 'b':6}),()).parse()
        # self.assertEquals(query, "update TestCoordinates  set b = 6, a = '5'")
        # query = Update('#12:2', Set({'a':5, 'b':6}),()).parse()
        # self.assertEquals(query, "update #12:2  set b = 6, a = 5")
        # query = Update(TestCoordinates, Set({'a':'5', 'b':6}),(Upsert())).parse()
        # self.assertEquals(query, "update TestCoordinates  set b = 6, a = '5'  upsert")
        pass

    def test_property(self):
        query = Create(Property(TestCoordinates, "land", OBinaryType.STRING)).parse()
        self.assertEquals(query, "create property TestCoordinates.land STRING")
        query = Create(Property.withlinkedclass(TestCoordinates, "land", OBinaryType.EMBEDDEDLIST, "TestLocation")).parse()
        self.assertEquals(query, "create property TestCoordinates.land EMBEDDEDLIST TestLocation")
        query = Create(Property.withlinkedtype(TestCoordinates, "land", OBinaryType.LINKLIST, OBinaryType.INTEGER)).parse()
        self.assertEquals(query, "create property TestCoordinates.land LINKLIST INTEGER")

    def test_delete(self):
        location = TestCoordinates()
        location.lat = 5
        location.lng = '10'
        location.setRID(5,5)

        testedge = TestEdgeOne()
        testedge.setRID(10,10)

        query = Delete(Class(TestCoordinates)).parse()
        self.assertEquals(query, "delete  vertex TestCoordinates")

        query = Delete(Class(TestEdgeOne)).parse()
        self.assertEquals(query, "delete  edge TestEdgeOne")

        query = Delete(Vertex).byRID('#12:2').parse()
        self.assertEquals(query, "delete vertex  #12:2 ")

        query = Delete(Edge).byRID('#10:10').parse()
        self.assertEquals(query, "delete edge  #10:10 ")

        query = Delete(Edge).toRID(testedge).parse()
        self.assertEquals(query, "delete edge  to #10:10 ")

        query = Delete(Edge).fromRID(testedge).parse()
        self.assertEquals(query, "delete edge  from #10:10 ")

        query = Delete(Edge).fromRID('#1:2').parse()
        self.assertEquals(query, "delete edge  from #1:2 ")

        query = Delete(Edge).toRID('#1:2').parse()
        self.assertEquals(query, "delete edge  to #1:2 ")

        query = Delete(Edge).fromRID('#2:3').toRID('#1:2').parse()
        self.assertEquals(query, "delete edge  from #2:3 to #1:2 ")

        query = Delete(location).parse()
        self.assertEquals(query, "delete  vertex #5:5 ")

        query = Delete(testedge).parse()
        self.assertEquals(query, "delete  edge #10:10 ")

        query = Delete(testedge, (Where(Condition("a").iseq(5)))).parse()
        self.assertEquals(query, "delete  edge #10:10    where a = 'b' ")

    def test_drop(self):
        query = Drop(Class(TestLocation)).parse()
        self.assertEquals(query, "drop class TestLocation")

        query = Drop(Property(TestLocation, "Test")).parse()
        self.assertEquals(query, "drop property TestLocation.Test")


if __name__ == "__main__":
    unittest.main()