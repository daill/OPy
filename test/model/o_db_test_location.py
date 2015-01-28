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

from test.model.o_db_test_city import TestCity
from test.model.o_db_test_coordinates import TestCoordinates
from test.model.o_db_test_obj import TestObject

__author__ = 'daill'

class TestLocation(TestObject):
    def __init__(self, name:str, city:TestCity, coordinates:TestCoordinates):
        self.name = name
        self.city = city
        self.coordinates = coordinates

    def persistent_attributes(self):
        return ['name', 'city', 'coordinates']

