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

from database.o_db_constants import OOperationType
from database.protocol.o_op import OOperation

__author__ = 'daill'

class ORequestShutdown(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_SHUTDOWN)

    self.__request_profile_str = ""
    self.__response_profile_str = "(protocol_number:byte)"

    self.__request_profile = None
    self.__response_profile = None

  def exec(self, connection):
    return super().exec(connection)

  def getrequestprofile(self):
    return super().getrequestprofile()

  def getresponseprofile(self):
    return super().getresponseprofile()

