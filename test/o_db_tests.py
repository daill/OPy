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

from database.o_db_connection import OConnection
from database.o_db_constants import ODBType, OModeChar, OCommandClass, OModeInt, ORecordType
from database.protocol.o_op_request import OSQLCommand
from database.o_db_ops import ODB


__author__ = 'daill'

class ODBTests(unittest.TestCase):
    def setUp(self):
        self.__odb = ODB()
        self.__connection = OConnection()
        print(self.__odb.connect(self.__connection, user_name="root", user_password="root"))
        print(self.__odb.dbopen(self.__connection, database_name="Test", database_type=ODBType.DOCUMENT.value, user_name="root", user_password="root"))


    def test_createdb(self):
        pass

    def test_create_record(self):
        strings = ['Alpha', 'Beta', 'Gamma', 'Delta', 'Omega']

        for string in strings:
            content = 'Profile@name:"{}", surname:"Zeta"'.format(string)
            print(self.__odb.recordcreate(self.__connection, mode=OModeInt.SYNCHRONOUS, record_content=content, record_type=ORecordType.DOCUMENT))


    def test_command(self):
        command = OSQLCommand("create class Profile extends V", non_text_limit=-1, fetchplan="*:0", serialized_params="")
        print(self.__odb.command(self.__connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command))

    def tearDown(self):
        self.__connection.close()

if __name__ == "__main__":
    unittest.main()