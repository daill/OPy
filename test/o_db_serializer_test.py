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
from common.o_db_model import OVarInteger
from database.o_db_codec import OCodec
from database.o_db_serializer import OBinarySerializer
from test.model.o_db_test_model import TestCity, TestLocation

__author__ = 'daill'


class ODBSerializerTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_varint_maxlong(self):
        value = 9223372036854775807
        bytes = OVarInteger().encode(value)
        pos, varint = OVarInteger().decode(bytes)
        self.assertEqual(value, varint)

    def test_varint_maxminus1(self):
        value = 9223372036854775806
        bytes = OVarInteger().encode(value)
        pos, varint = OVarInteger().decode(bytes)
        self.assertEqual(value, varint)

    def test_varint(self):
        value = 300
        bytes = OVarInteger().encode(value)
        pos, varint = OVarInteger().decode(bytes)
        self.assertEqual(value, varint)

    def test_varint_one(self):
        value = 1
        bytes = OVarInteger().encode(value)
        pos, varint = OVarInteger().decode(bytes)
        self.assertEqual(value, varint)

    def test_varint_three(self):
        value = 3
        bytes = OVarInteger().encode(value)
        pos, varint = OVarInteger().decode(bytes)
        self.assertEqual(value, varint)

    def test_embeddedmap(self):
        values = {'a':1, 'b':2, 'c':3}
        codec = OCodec()
        bytes = codec.writeembeddedmap(values)
        result = codec.readembeddedmap(bytes)
        self.assertEqual(values, result[0])

    def test_simple_binary_serialization(self):

        city = TestCity()
        city.name = "Kassel"
        serializer = OBinarySerializer()

        bytes = serializer.encode(city)

        record, name = serializer.decode(bytes)
        result_city = serializer.toobject(name, record)

        self.assertEquals(city.__class__, result_city.__class__)
        self.assertEquals(city.name, result_city.name)

    def test_advanced_binary_serialization(self):
        city = TestCity()
        city.name = "Kassel"

        location = TestLocation()
        location.city = city

        serializer = OBinarySerializer()

        bytes = serializer.encode(location)

        record, name, rest = serializer.decode(bytes)
        result_location = serializer.toobject(name, record)

        self.assertEquals(location.__class__, result_location.__class__)
        self.assertEquals(location.city.__class__, result_location.city.__class__)
        self.assertEquals(location.city.name, result_location.city.name)

if __name__ == "__main__":
    unittest.main()