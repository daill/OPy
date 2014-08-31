import socket
import logging
import struct
import sys
import builtins
from database.o_db_exceptions import NotConnectedException, WrongTypeException

from database.o_db_constants import OTypes, OConst, ODriver
from database.o_db_profile_parser import OCondition
from database.protocol.o_op import OOperation
from database.protocol.o_op_connect import OOperationConnect
from database.protocol.o_op_db import OOperationDBClose, OOperationDBOpen
from database.protocol.o_op_error import OOperationError
from database.protocol.o_op_init import OOperationInit


__author__ = 'daill'

class OProfileEncoder(object):
  def encode(self, operation: OOperation, arguments: dict):

    def pack_data(type, value):
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
      #   if isinstance(value, builtins.bytes):
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
          bytes += struct.pack(">i",len(value))
          for string in value:
            string_length = len(string)
            bytes += struct.pack(">i {}s".format(string_length), string_length, string.encode('utf-8'))
          return bytes
        else:
          raise WrongTypeException("wrong value type for {} type".format(type))

    return operation.encode(pack_data, arguments)


class OProfileDecoder(object):
  def decode(self, operation: OOperation, data: bytes):

    def unpack_data(type, data, condition: OCondition=None):
      logging.debug("parsing type {}".format(type))

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
        value, _data = struct.unpack('>{}s'.format(count), _data[:count])[0], _data[count:]
        return _data, value
      elif type == OTypes.STRING.value:
        int_length = 4
        string_length, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
        value, _data = struct.unpack('>{}s'.format(string_length), _data[:string_length])[0], _data[string_length:]
        return _data, value
      elif type == OTypes.RECORDS.value:
        # TODO: implement
        pass
      elif type == OTypes.STRINGS.value:
        int_length = 4
        strings_count, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
        result = list()
        for i in range(strings_count):
          string_length, _data = struct.unpack('>i', data[:int_length])[0], data[int_length:]
          value, _data = struct.unpack('>{}s'.format(string_length), data[:string_length])[0], _data[string_length:]
          result.append(value)

        return _data, value

    data_dict, status = operation.decode(unpack_data, data)

    # handle error
    if status == OConst.ERROR and not isinstance(operation, OOperationError):
      error_operation = OOperationError()
      data_dict, status = error_operation.decode(unpack_data, data)
      logging.debug("error data: %s",data_dict)
      raise NotConnectedException("connection not established", data_dict)

    return data_dict


class OConnection(object):
  def __init__(self,user, password, host='localhost', port=2424):
    self.__host = host
    self.__port = port

    self.__protocol_version = None
    self.__sock = None
    self.__session_id = None

    self.open()

  def receive(self):
    data = bytes()
    while True:
      try:

        buffer = self.__sock.recv(4096)
        data += buffer
        if len(buffer) < 4096:
          break
        if not buffer:
          break
      except socket.error as msg:
        logging.error(msg)
    return data

  def exec(self, operation: OOperation, data: dict):
    # before sending request we need to append the operation type and session id
    # send request
    # receive response

    logging.debug("execute {}".format(operation.__class__))

    if self.__sock is None:
      logging.error("execution of {} failed".format(operation.__class__))
      raise NotConnectedException("the socket connection it not open")

    request_bytes = self.get_request_head(operation.get_operation_type())
    request_bytes += self.parse_request(operation, data)

    self.__sock.sendall(request_bytes)

    if not isinstance(operation,OOperationDBClose):
      data = self.receive()
      parsed_data = self.parse_response(operation, data)
      if isinstance(operation,OOperationConnect) or isinstance(operation, OOperationDBOpen):
        self.__session_id = parsed_data["session-id"]
        logging.debug("session id {} saved".format(self.__session_id))
      return parsed_data

    return None


  def get_request_head(self, operation_type):
    """
    Prepares the head of the request, consisting of operation type and session id
    :return:
    """
    if self.__session_id is None:
      head = struct.pack(">b i", operation_type, -1)
    else:
      head = struct.pack(">b i",operation_type, self.__session_id)

    return head;

  def get_session_id(self):
    return self.__session_id

  def get_protocol_version(self):
    return self.__protocol_version

  def parse_request(self, operation: OOperation, data: dict):
    logging.debug("parse request for {} operation".format(operation.__class__))
    parser = OProfileEncoder()
    return parser.encode(operation, data)


  def parse_response(self, operation: OOperation, data):
    logging.debug("parse response for {} operation".format(operation.__class__))

    parser = OProfileDecoder()
    return parser.decode(operation, data)

  def close(self):
    try:
      self.__sock.close()
      logging.info("socket closed")
    except Exception as err:
      logging.error("could not close connection: {}".format(err))


  def open(self):
    try:
      logging.debug("opening connection")
      self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.__sock.connect((self.__host, self.__port))

      data = self.receive()
      operation_init = OOperationInit()
      result = self.parse_response(operation_init, data)

      self.__protocol_version = result['protocol_number']
      logging.info("working with protocol version {}".format(self.__protocol_version))
      logging.debug("connection opened")
    except socket.error as msg:
      logging.error('Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1])
      self.__sock.close()
      sys.exit()
    except NotConnectedException as err:
      logging.error("could not open connection: {}".format(err))
      self.__sock.close()

