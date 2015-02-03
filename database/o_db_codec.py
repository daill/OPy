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
from datetime import datetime

import logging
import struct
from common.o_db_exceptions import WrongTypeException, NotConnectedException
from common.o_db_model import ORidBagBinary, OVarInteger
from database.o_db_constants import OProfileType, OConst, OBinaryType
from database.o_db_profile_parser import OCondition
from database.protocol.o_op import OOperation
from database.protocol.o_op_error import OOperationError

__author__ = 'daill'


class OCodec(object):
    def __init__(self):
        self.position = 0

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

    def unpackdata(self, type, data, condition: OCondition=None, name=""):
        logging.debug("unpacking '{}' with type {}".format(name, type))

        if type == OProfileType.BOOLEAN:
            result, rest = self.readboolean(data)
            return rest, (result == 1 if True else False)
        elif type == OProfileType.BYTE:
            result, rest = self.readbyte(data)
            return rest, result
        elif type == OProfileType.BYTE_STATIC:
            byte, rest = self.readbyte(data)
            if condition is not None:
                if condition.eval(byte):
                    return rest, byte
                else:
                    return data, byte
            else:
                return rest, byte
        elif type == OProfileType.SHORT:
            result, rest = self.readshort(data)
            return rest, result
        elif type == OProfileType.INT:
            result, rest = self.readint(data)
            return rest, result
        elif type == OProfileType.LONG:
            result, rest = self.readlong(data)
            return rest, result
        elif type == OProfileType.BYTES:
            count, rest = self.readint(data)
            if count > 0:
                value, rest = self.readbytes(count, rest)
            else:
                value = 0
            return rest, value
        elif type == OProfileType.STRING:
            result, rest = self.readstring(data)
            return rest, result
        elif type == OProfileType.RECORD:
            # every possible record is beginning with a short value which decides how to handle the record
            # see ORecord for further informations
            result, rest = self.readshort(data)
            return rest, result
        elif type == OProfileType.STRINGS:
            int_length = 4
            strings_count, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
            result = list()
            for i in range(strings_count):
                string_length, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
                value, _data = struct.unpack('>{}s'.format(string_length), data[:string_length])[0], _data[string_length:]
                result.append(value)

            return _data, value

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

    def readvarint(self, data):
        pos, varint = OVarInteger().decode(data)
        rest = data[pos:]
        return varint, rest

    def readvarintstring(self, data):
        class_name_length, rest = self.readvarint(data)
        class_name, rest = struct.unpack('>{}s'.format(class_name_length), rest[:class_name_length])[0], rest[class_name_length:]
        return class_name, rest

    def readbyte(self, data):
        self.position+=1
        return struct.unpack('>b', data[:1])[0], data[1:]

    def readbytes(self, length, data):
        self.position+=length
        return struct.unpack('>{}s'.format(length), data[:length])[0], data[length:]

    def readshort(self, data):
        self.position+=2
        return struct.unpack('>h', data[:2])[0], data[2:]

    def readint(self, data):
        self.position+=4
        return struct.unpack('>i', data[:4])[0], data[4:]

    def readlong(self, data):
        self.position+=8
        return struct.unpack('>q', data[:8])[0], data[8:]

    def readfloat(self, data):
        self.position+=4
        return struct.unpack('>f', data[:4])[0], data[4:]

    def readdouble(self, data):
        self.position+=8
        return struct.unpack('>d', data[:8])[0], data[48]

    def readboolean(self, data):
        return self.readbyte(data)

    def readdatetime(self, data):
        timestamp, rest = self.readvarint(data)
        return datetime.fromtimestamp(timestamp), rest

    def readdate(self, data):
        milliseconds_per_day = 86400000
        time, rest = self.readlong(data)
        local_timezone_offset = (datetime.now().hour-datetime.now(datetime.timezone.utc).hour)*3600000
        return time*milliseconds_per_day+local_timezone_offset, rest

    def readembedded(self, data):
        return self.decode(data)

    def readbinary(self, data):
        length, rest = self.readvarint(data)
        self.position+=length
        return self.readbytes(length, rest)

    def readembeddedcollection(self, data):
        length, rest = self.readvarint(data)
        type, rest = self.readbyte(rest)

        result = list()

        if OBinaryType(type) == OBinaryType.ANY:
            for i in range(length):
                subtype, rest = self.readbyte(rest)
                if OBinaryType(subtype) == OBinaryType.ANY:
                    # do nothing
                    pass
                else:
                    object, rest = self.readvalue(subtype, rest)
                    result.append(object)

        return result, rest

    def readembeddedlist(self, data):
        return self.readembeddedcollection(data)

    def readembeddedset(self, data):
        return self.readembeddedcollection(data)

    def readembeddedmap(self, data):
        size, rest = self.readvarint(data)
        result = dict()
        delta = 0

        for i in range(size):
            key_type, rest = self.readbyte(rest)
            key, rest = self.readvalue(key_type, rest)
            pos, rest = self.readint(rest)
            value_type, rest = self.readbyte(rest)

            # due to the implementation it necessary to calculate the actual position in
            # byte array to extract the value of each key. Afterwards we have to cumulate the
            # read bytes to receive the complete amount of read bytes

            estimated_position = pos-self.position
            last_position = self.position

            value, temp_rest = self.readvalue(value_type, rest[estimated_position:])
            delta += self.position-estimated_position
            
            self.position = last_position

            result[key] = value

        # set the count to real read position
        self.position += delta


        return result, rest

    def readlink(self, data):
        cluster_id, rest = self.readvarint(data)
        position, rest = self.readvarint(rest)

        return (cluster_id, position), rest

    def readlinkset(self, data):
        length, rest = self.readvarint(data)
        result = list()
        for i in range(length):
            rid, rest = self.readlink(rest)
            result.append(rid)

        return result, rest

    def readstring(self, data):
        length, rest = self.readint(data)
        value, rest = self.readbytes(length, rest)

        return value, rest

    def readlinklist(self, data):
        return self.readlinkset(data)

    def readlinkmap(self, data):
        size, rest = self.readvarint(data)
        result = dict()

        for i in range(size):
            keytype, rest = self.readbyte(data)
            key, rest = self.readvalue(keytype, rest)
            rid, rest = self.readlink(rest)
            result[key] = rid

        return result, rest

    def readuuid(self, data):
        most_sig_bits, rest = self.readlong(data)
        least_sig_bits, rest = self.readlong(rest)
        return (most_sig_bits, least_sig_bits), rest

    def readembeddedridbag(self, data):
        logging.debug("read embeddedridbag")
        value, rest = self.readint(data)
        content_size = value * 10 + 4
        size, rest = self.readint(rest)

        content, rest = self.readbytes(content_size, rest)

        ridbag = ORidBagBinary()
        ridbag.size = size
        ridbag.content = content

        # deserialize the content
        actual_position = self.position
        self.position = 0

        entries_size, content_rest = self.readint(content)
        entries = list()

        for i in range(entries_size):
            link, content_rest = self.readlink(content_rest)
            entries.append(link)

            # should we read all the records here?
            logging.debug("automatic record loading is not yet implemented")

        ridbag.entries = entries
        self.position = actual_position

        return ridbag, rest


    def readridbag(self, data):
        """
        SBTree has not been implemented due to problems with remote connections

        :param data:
        :return:
        """
        logging.debug("read ridbag")

        if (type & 2) == 2:
            logging.debug("read uuid")
            # reads the uuid tuple
            uuid, rest = self.readuuid(data)

        type, rest = self.readbyte(data)
        if (type & 1) == 1:
            logging.debug("read embedded")
            # embedded
            ridbag, rest = self.readembeddedridbag(data)
        else:
            logging.debug("read tree")
            # tree
            pass




    def readvalue(self, type, data):
        """
        TODO: implement RidBag

        :param type:
        :param data:
        :return:
        """
        otype = OBinaryType(type)

        if otype == OBinaryType.BOOLEAN:
            return self.readbyte(data)
        elif otype == OBinaryType.INTEGER:
            return self.readint(data)
        elif otype == OBinaryType.SHORT:
            return self.readshort(data)
        elif otype == OBinaryType.LONG:
            return self.readlong(data)
        elif otype == OBinaryType.STRING:
            return self.readstring(data)
        elif otype == OBinaryType.DOUBLE:
            return self.readdouble(data)
        elif otype == OBinaryType.FLOAT:
            return self.readfloat(data)
        elif otype == OBinaryType.BYTE:
            return self.readbyte(data)
        elif otype == OBinaryType.BINARY:
            return self.readbinary(data)
        elif otype == OBinaryType.DATE:
            return self.readdate(data)
        elif otype == OBinaryType.DATETIME:
            return self.readdatetime(data)
        elif otype == OBinaryType.EMBEDDED:
            return self.readembedded(data)
        elif otype == OBinaryType.EMBEDDEDLIST:
            return self.readembeddedlist(data)
        elif otype == OBinaryType.EMBEDDEDMAP:
            return self.readembeddedmap(data)
        elif otype == OBinaryType.EMBEDDEDSET:
            return self.readembeddedset(data)
        elif otype == OBinaryType.LINK:
            return self.readlink(data)
        elif otype == OBinaryType.LINKSET:
            return self.readlinkset(data)
        elif otype == OBinaryType.LINKLIST:
            return self.readlinklist(data)
        elif otype == OBinaryType.LINKMAP:
            return self.readlinkmap(data)
        elif otype == OBinaryType.LINKBAG:
            return self.readridbag(data)

