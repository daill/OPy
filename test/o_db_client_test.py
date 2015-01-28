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
from client.o_db_set import Select, Class, Where, Condition, OrderBy, Let, QueryElement, GroupBy, Insert
from test.model.o_db_test_coordinates import TestCoordinates
from test.model.o_db_test_location import TestLocation
from test.model.o_db_test_obj import TestObject

__author__ = 'daill'

class ODBClientTests(unittest.TestCase):

    def test_selectwhere(self):
        query = Select(Class("Location l"),["l.a", "l.b"], Where(Condition("l.a").iseq("b"))).parse()
        self.assertEqual(query, "select l.a l.b from Location l where l.a = b;")

    def test_selectorderofelements(self):
        query = Select(Class("Location l"),["l.a", "l.b"], OrderBy("a").asc(), Where(Condition("l.a").iseq("b"))).parse()
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

    def test_insert(self):
        location = TestCoordinates(5,'4')
        query = Insert(location).parse()
        self.assertEqual(query, "insert into TestLocation ( lat, lng ) values ('5','4')")


if __name__ == "__main__":
    unittest.main()