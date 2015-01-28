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

import logging
import struct
from common.o_db_exceptions import WrongTypeException, NotConnectedException
from database.o_db_constants import OProfileType, OConst
from database.o_db_profile_parser import OCondition
from database.protocol.o_op import OOperation
from database.protocol.o_op_error import OOperationError

__author__ = 'daill'


class OCodec(object):
    def packdata(self, type, value, name=" "):
        if not 'pass' in name:
            logging.debug("packing '{}' with type {} and value '{}'".format(name, type, value))

        if type == OProfileType.BOOLEAN:
            return struct.pack(">b", value)
        elif type == OProfileType.BYTE:
            if isinstance(value, str):
                return struct.pack(">c", value.encode("utf-8"))
            else:
                return struct.pack(">b", value)
        elif type == OProfileType.SHORT:
            return struct.pack(">h", value)
        elif type == OProfileType.INT:
            return struct.pack(">i", value)
        elif type == OProfileType.LONG:
            return struct.pack(">q", value)
            # elif type == OTypes.BYTES.value:
            # if isinstance(value, builtins.bytes):
            #     length = len(value)
            #       return struct.pack(">i {}s".format(length), length, value)
            #   else:
            #     raise WrongTypeException("wrong value type for {} type".format(type))
        elif type == OProfileType.STRING or type == OProfileType.BYTES:
            if value == '-1' or value == -1 and type == OProfileType.STRING:
                return struct.pack(">i", -1)
            elif isinstance(value, str):
                length = len(value)
                return struct.pack(">i {}s".format(length), length, value.encode('utf-8'))
            else:
                raise WrongTypeException("wrong value type for {} type".format(type))
        elif type == OProfileType.RECORDS:
            # TODO: implement
            pass
        elif type == OProfileType.STRINGS:
            if isinstance(value, list):
                bytes = ''
                bytes += struct.pack(">i", len(value))
                for string in value:
                    string_length = len(string)
                    bytes += struct.pack(">i {}s".format(string_length), string_length, string.encode('utf-8'))
                return bytes
            else:
                raise WrongTypeException("wrong value type for {} type".format(type))
        elif type == OProfileType.DYNAMIC:

            pass
        elif type == OProfileType.STRING_VARINT:
            # the length of this special string will be delivered via varint
            struct.pack(">b", value)


    def unpackdata(self, type, data, condition: OCondition=None, name=""):
        logging.debug("unpacking '{}' with type {}".format(name, type))

        if type == OProfileType.BOOLEAN:
            length = 1
            return data[:length], (struct.unpack('>b', data[:length])[0] == 1 if True else False)
        elif type == OProfileType.BYTE:
            length = 1
            return data[length:], struct.unpack('>b', data[:length])[0]
        elif type == OProfileType.BYTE_STATIC:
            length = 1
            byte = struct.unpack('>b', data[:length])[0]
            if condition is not None:
                if condition.eval(byte):
                    return data[length:], byte
                else:
                    return data, byte
            else:
                return data[length:], byte
        elif type == OProfileType.SHORT:
            length = 2
            return data[length:], struct.unpack('>h', data[:length])[0]
        elif type == OProfileType.INT:
            length = 4
            return data[length:], struct.unpack('>i', data[:length])[0]
        elif type == OProfileType.LONG:
            length = 8
            return data[length:], struct.unpack('>q', data[:length])[0]
        elif type == OProfileType.BYTES:
            int_length = 4
            count, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
            if count > 0:
                value, _data = struct.unpack('>{}s'.format(count), _data[:count])[0], _data[count:]
            else:
                value = 0
            return _data, value
        elif type == OProfileType.STRING:
            int_length = 4
            string_length, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
            value, _data = struct.unpack('>{}s'.format(string_length), _data[:string_length])[0], _data[string_length:]
            return _data, value
        elif type == OProfileType.RECORD:
            # every possible record is beginning with a short value which decides how to handle the record
            # see ORecord for further informations
            length = 2
            return data[length:], struct.unpack('>h', data[:length])[0]
        elif type == OProfileType.STRINGS:
            int_length = 4
            strings_count, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
            result = list()
            for i in range(strings_count):
                string_length, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
                value, _data = struct.unpack('>{}s'.format(string_length), data[:string_length])[0], _data[string_length:]
                result.append(value)

            return _data, value
        elif type == OProfileType.VARINT:
            pass

    def encode(self, operation: OOperation, arguments: dict):
        return operation.encode(self.packdata, arguments)

    def decode(self, operation: OOperation, data: bytes):

        data_dict, status = operation.decode(self.unpackdata, data)

        # handle error
        if status == OConst.ERROR and not isinstance(operation, OOperationError):
            error_operation = OOperationError()
            data_dict, status = error_operation.decode(self.unpackdata, data)
            logging.debug("error data: %s", data_dict)
            raise NotConnectedException("connection not established", data_dict)

        return data_dict