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

from opy.client.o_db_base import BaseVertex
from opy.client.o_db_base import BaseEdge

__author__ = 'daill'

class TestEdgeOne(BaseEdge):
    def __init__(self):
        pass

class TestEdgeTwo(BaseEdge):
    def __init__(self):
        pass


class TestObject(BaseVertex):
    def __init__(self):
        pass

class TestCoordinates(TestObject):
    def __init__(self):
        self.lat = None
        self.lng = None

    def persistentattributes(self):
        return ['lat', 'lng']


class TestCity(TestObject):
    def __init__(self):
        self.name = None

    def persistentattributes(self):
        return ['name']


class TestLocation(TestObject):
    def __init__(self):
        self.name = None
        self.city = None
        self.coordinates = None

    def persistentattributes(self):
        return ['name', 'city', 'coordinates']


