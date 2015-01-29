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

from database.o_db_constants import OSerialization

__author__ = 'daill'


class ODriverConfig(object):
    DRIVER_NAME = 'OPy'
    DRIVER_VERSION = '0.1'
    SERIALIZATION = OSerialization.SERIALIZATION_BINARY