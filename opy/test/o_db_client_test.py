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

from opy.client.o_db_set import Select, Class, Where, Condition, OrderBy, Let, GroupBy, Insert, Create, Vertex, Property, Delete, And, Or, Drop, Edge, Index, Prefixed, Move, Cluster, Traverse, While, \
    Limit
from opy.common.o_db_constants import OBinaryType, OSQLIndexType, OPlainClass
from opy.test.model.o_db_test_model import TestLocation, TestCoordinates, TestEdgeOne


__author__ = 'daill'

class ODBClientTests(unittest.TestCase):

    def test_select(self):
        query = Select(TestLocation, (), ()).parse()
        self.assertEqual(query, "select from TestLocation")

        query = Select(TestLocation, ["name"], ()).parse()
        self.assertEqual(query, "select name from TestLocation")

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

        query = Select(Prefixed(TestCoordinates, 'l'),["l.a", "l.b"], OrderBy.asc("a"), Where(Condition("l.a").iseq("b"))).parse()
        self.assertEqual(query, "select l.a, l.b from TestCoordinates l  where l.a = 'b'   order by a asc ")

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

    def test_index(self):
        query = Create(Index("test").on(TestCoordinates, ["id", "bla", "hallo"])).parse()
        self.assertEquals(query, "create index test on TestCoordinates (id, bla, hallo) ")

        query = Create(Index("test").on(TestCoordinates, ["id", "bla", "hallo"]).withmeta("{lala: false}")).parse()
        self.assertEquals(query, "create index test on TestCoordinates (id, bla, hallo)  metadata {lala: false}")

        query = Create(Index("id").on(TestCoordinates)).parse()
        self.assertEquals(query, "create index TestCoordinates.id")

        query = Create(Index("id").on(TestCoordinates, None, OSQLIndexType.UNIQUE)).parse()
        self.assertEquals(query, "create index TestCoordinates.id unique")

    def test_create(self):
        query = Create(Class(TestCoordinates, OPlainClass.VERTEX)).parse()
        self.assertEquals(query, "create class TestCoordinates extends V")

        query = Create(Class(TestEdgeOne, OPlainClass.EDGE)).parse()
        self.assertEquals(query, "create class TestEdgeOne extends E")


    def test_move(self):
        query = Move("#12:2", Class(TestLocation)).parse()
        self.assertEquals(query, "move vertex #12:2 to class: TestLocation")

        query = Move("#12:2", Cluster("testcluster")).parse()
        self.assertEquals(query, "move vertex #12:2 to cluster: testcluster")

    def test_travers(self):
        query = Traverse("#12:2", ['a', 'b'], ()).parse()
        self.assertEquals(query, "traverse a, b  from #12:2 ")

        query = Traverse(Class(TestLocation), ['a', 'b'], ()).parse()
        self.assertEquals(query, "traverse a, b  from TestLocation ")

        query = Traverse(Cluster("testcluster"), ['a', 'b'], ()).parse()
        self.assertEquals(query, "traverse a, b  from testcluster ")

        query = Traverse([Cluster("testcl1"), Cluster("testcl2")], ['a', 'b'], ()).parse()
        self.assertEquals(query, "traverse a, b  from testcl1, testcl2 ")

        query = Traverse(["#13:4", "#12:4"], ['a', 'b'], ()).parse()
        self.assertEquals(query, "traverse a, b  from #13:4, #12:4 ")

        query = Traverse(["#13:4", "#12:4"], ['a', 'b'], While(Condition("a").iseq("b"))).parse()
        self.assertEquals(query, "traverse a, b  from #13:4, #12:4   while a = 'b' ")

        query = Traverse(["#13:4", "#12:4"], ['a', 'b'], Where(Condition("a").iseq("b")), Limit(1)).parse()
        self.assertEquals(query, "traverse a, b  from #13:4, #12:4   limit 1 ")

        query = Traverse(Select(TestLocation, (), Where(Or(Condition("name").iseq("Eddies"),Condition("type").iseq("Pizaaria")))), ['a', 'b']).parse()
        self.assertEquals(query, "traverse a, b  from  ( select from TestLocation  where  ( name = 'Eddies'  or type = 'Pizaaria'  )   ) ")

if __name__ == "__main__":
    unittest.main()