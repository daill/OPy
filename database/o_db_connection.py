import socket
import logging
import struct
import sys
import time
import select

from common.o_db_exceptions import NotConnectedException, WrongTypeException

from database.o_db_constants import OTypes, OConst, OOperationType
from database.o_db_profile_parser import OCondition, OProfileParser
from database.protocol.o_op import OOperation
from database.protocol.o_op_connect import OOperationConnect
from database.protocol.o_op_db import OOperationDBClose, OOperationDBOpen
from database.protocol.o_op_error import OOperationError
from database.protocol.o_op_init import OOperationInit


__author__ = 'daill'


class OProfileEncoder(object):
    def encode(self, operation: OOperation, arguments: dict):

        def packdata(type, value, name=" "):
            if not 'pass' in name:
                logging.debug("packing '{}' with type {} and value '{}'".format(name, type, value))

            if type == OTypes.BOOLEAN.value:
                return struct.pack(">b", value)
            elif type == OTypes.BYTE.value:
                if isinstance(value, str):
                    return struct.pack(">c", value.encode("utf-8"))
                else:
                    return struct.pack(">b", value)
            elif type == OTypes.SHORT.value:
                return struct.pack(">h", value)
            elif type == OTypes.INT.value:
                return struct.pack(">i", value)
            elif type == OTypes.LONG.value:
                return struct.pack(">q", value)
                # elif type == OTypes.BYTES.value:
                # if isinstance(value, builtins.bytes):
                #     length = len(value)
                #       return struct.pack(">i {}s".format(length), length, value)
                #   else:
                #     raise WrongTypeException("wrong value type for {} type".format(type))
            elif type == OTypes.STRING.value or type == OTypes.BYTES.value:
                if value == '-1' or value == -1 and type == OTypes.STRING.value:
                    return struct.pack(">i", -1)
                elif isinstance(value, str):
                    length = len(value)
                    return struct.pack(">i {}s".format(length), length, value.encode('utf-8'))
                else:
                    raise WrongTypeException("wrong value type for {} type".format(type))
            elif type == OTypes.RECORDS.value:
                # TODO: implement
                pass
            elif type == OTypes.STRINGS.value:
                if isinstance(value, list):
                    bytes = ''
                    bytes += struct.pack(">i", len(value))
                    for string in value:
                        string_length = len(string)
                        bytes += struct.pack(">i {}s".format(string_length), string_length, string.encode('utf-8'))
                    return bytes
                else:
                    raise WrongTypeException("wrong value type for {} type".format(type))
            elif type == OTypes.DYNAMIC.value:

                pass

        return operation.encode(packdata, arguments)


class OProfileDecoder(object):
    def decode(self, operation: OOperation, data: bytes):

        def unpackdata(type, data, condition: OCondition=None, name=""):
            logging.debug("unpacking '{}' with type {}".format(name, type))

            if type == OTypes.BOOLEAN.value:
                length = 1
                return data[:length], (struct.unpack('>b', data[:length])[0] == 1 if True else False)
            elif type == OTypes.BYTE.value:
                length = 1
                return data[length:], struct.unpack('>b', data[:length])[0]
            elif type == OTypes.BYTE_STATIC.value:
                length = 1
                byte = struct.unpack('>b', data[:length])[0]
                if condition is not None:
                    if condition.eval(byte):
                        return data[length:], byte
                    else:
                        return data, byte
                else:
                    return data[length:], byte
            elif type == OTypes.SHORT.value:
                length = 2
                return data[length:], struct.unpack('>h', data[:length])[0]
            elif type == OTypes.INT.value:
                length = 4
                return data[length:], struct.unpack('>i', data[:length])[0]
            elif type == OTypes.LONG.value:
                length = 8
                return data[length:], struct.unpack('>q', data[:length])[0]
            elif type == OTypes.BYTES.value:
                int_length = 4
                count, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
                if count > 0:
                    value, _data = struct.unpack('>{}s'.format(count), _data[:count])[0], _data[count:]
                else:
                    value = 0
                return _data, value
            elif type == OTypes.STRING.value:
                int_length = 4
                string_length, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
                value, _data = struct.unpack('>{}s'.format(string_length), _data[:string_length])[0], _data[string_length:]
                return _data, value
            elif type == OTypes.RECORD.value:
                # every possible record is beginning with a short value which decides how to handle the record
                # see ORecord for further informations
                length = 2
                return data[length:], struct.unpack('>h', data[:length])[0]
            elif type == OTypes.STRINGS.value:
                int_length = 4
                strings_count, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
                result = list()
                for i in range(strings_count):
                    string_length, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
                    value, _data = struct.unpack('>{}s'.format(string_length), data[:string_length])[0], _data[string_length:]
                    result.append(value)

                return _data, value

        data_dict, status = operation.decode(unpackdata, data)

        # handle error
        if status == OConst.ERROR and not isinstance(operation, OOperationError):
            error_operation = OOperationError()
            data_dict, status = error_operation.decode(unpackdata, data)
            logging.debug("error data: %s", data_dict)
            raise NotConnectedException("connection not established", data_dict)

        return data_dict


class OConnection(object):
    def __init__(self, host:str='0.0.0.0', port:int=2424):
        self.__host = host
        self.__port = port

        self.__protocol_version = None
        self.__sock = None
        self.__session_id = None
        self.__token = None
        self.__token_based = False

        self.__buffer_size = 4096

        # set timeout to 1 second
        self.__initial_timeout = 1
        self.__short_timeout = 0.01
        self.__timeout_inc = 0.01
        self.__timeout_dec = 0.01
        self.__minimum_timeout = 0.00001


        self.open()

    def isopen(self):
        return self.__sock is not None

    def receive(self):
        data = bytes()

        # initial retry count, set relatively high to let the server do his work
        retry_count = 3

        timeout = self.__initial_timeout

        start = time.time()

        while True:
            try:
                # initially wait max 1 second, after there has been some byte read, we can decrease the timeout
                read,write,excpt = select.select([self.__sock],[],[], timeout)

                if self.__sock in read:
                    buffer = self.__sock.recv(self.__buffer_size)

                    logging.debug("read {} bytes on retry count {} with timeout {}s".format(len(buffer), retry_count, timeout))

                    if buffer:
                        data += buffer
                    else:
                        break

                    timeout = self.__short_timeout

                    if len(buffer) < self.__buffer_size:
                        # in case of a not full buffer increase timeout a bit
                        timeout += self.__timeout_inc
                    else:
                        # in case of a full buffer slightly decrease timeout time
                        timeout -= self.__timeout_dec

                        # be aware of negative timeouts
                        if timeout < 0:
                            timeout = self.__minimum_timeout

                    retry_count = 3
                else:
                    # if there were no bytes to read, try a bit longer timeout
                    time.sleep(0.1)
                    retry_count -= 1

                    if retry_count == 0:
                        break
            except socket.error as e:
                logging.error("socket error: " + e)
        end = time.time()
        logging.debug("total runtime {}s".format(end-start))
        return data

    def exec(self, operation: OOperation, data: dict):
        # before sending request we need to append the operation type and session id
        # send request
        # receive response

        logging.debug("execute {}".format(operation.__class__))

        if self.__sock is None:
            logging.error("execution of {} failed".format(operation.__class__))
            raise NotConnectedException("the socket connection it not open")

        if isinstance(operation, OOperationConnect):
            if "token-session" in data and data["token-session"] == 1:
                self.__token_based = True


        request_bytes = self.getrequesthead(operation.getoperationtype())
        request_bytes += self.parserequest(operation, data)

        self.__sock.sendall(request_bytes)

        if not isinstance(operation, OOperationDBClose):
            data = self.receive()
            logging.debug("read {}".format(data))
            parsed_data = self.parseresponse(operation, data)
            if isinstance(operation, OOperationConnect) or isinstance(operation, OOperationDBOpen):
                self.__session_id = parsed_data["session-id"]
                logging.debug("session id {} saved".format(self.__session_id))

                if "token" in parsed_data:
                    self.__token = parsed_data["token"]
                    logging.debug("token {} saved".format(self.__token))
            return parsed_data

        return None

    def sendbytes(self, bytes):
        """
        Use this method i.e. to cancel a running transaction
        :param bytes: data to send
        """
        if self.__sock is not None:
            self.__sock.sendall(bytes)


    def getrequesthead(self, operation_type):
        """
        Prepares the head of the request, consisting of operation type and session id
        :return:
        """
        if self.__session_id is None:
            head = struct.pack(">b i", operation_type, -1)
        else:
            head = struct.pack(">b i", operation_type, self.__session_id)

        if self.__token_based:
            if operation_type != OOperationType.REQUEST_CONNECT.value and operation_type != OOperationType.REQUEST_DB_OPEN.value:
                length = len(self.__token)
                head += struct.pack(">i {}s".format(length), length, self.__token)

        return head

    def getsessionid(self):
        return self.__session_id

    def getprotocolversion(self):
        return self.__protocol_version

    def gettoken(self):
        return self.__token

    def parserequest(self, operation: OOperation, data: dict):
        logging.debug("parse request for {} operation".format(operation.__class__))
        parser = OProfileEncoder()

        return parser.encode(operation, data)

    def parseresponse(self, operation: OOperation, data):
        logging.debug("parse response for {} operation".format(operation.__class__))

        parser = OProfileDecoder()

        if isinstance(operation, OOperationConnect) or isinstance(operation, OOperationDBOpen):
            operation.token_based = False
        else:
            operation.token_based = self.__token_based

        return parser.decode(operation, data)

    def close(self):
        try:
            self.__sock.close()
            logging.info("socket closed")
        except Exception as err:
            logging.error("could not close connection: {}".format(err))


    def open(self):
        try:
            if self.__sock is None:
                logging.debug("opening connection")
                self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.__sock.connect((self.__host, self.__port))
                self.__sock.setblocking(0)

                operation_init = OOperationInit()
                data = self.receive()
                result = self.parseresponse(operation_init, data)

                self.__protocol_version = result['protocol_number']
                logging.info("working with protocol version {}".format(self.__protocol_version))
            else:
                logging.debug("connection already opened")
            logging.debug("connection opened")
        except socket.error as msg:
            logging.error('Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1])
            self.__sock.close()
            sys.exit()
        except NotConnectedException as err:
            logging.error("could not open connection: {}".format(err))
            self.__sock.close()


