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

import socket
import logging
import struct
import sys
import time
import select

from common.o_db_exceptions import NotConnectedException
from database.o_db_codec import OCodec
from database.o_db_constants import OOperationType
from database.protocol.o_op import OOperation
from database.protocol.o_op_connect import OOperationConnect
from database.protocol.o_op_db import OOperationDBClose, OOperationDBOpen
from database.protocol.o_op_init import OOperationInit


__author__ = 'daill'

class OConnection(object):
    def __init__(self, host:str='0.0.0.0', port:int=2424):
        self.__host = host
        self.__port = port

        self.protocol_version = None
        self.__sock = None
        self.session_id = None
        self.token = None
        self.token_based = False

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
                self.token_based = True


        request_bytes = self.getrequesthead(operation.getoperationtype())
        request_bytes += self.parserequest(operation, data)

        self.__sock.sendall(request_bytes)

        if not isinstance(operation, OOperationDBClose):
            data = self.receive()
            logging.debug("read {}".format(data))
            parsed_data = self.parseresponse(operation, data)
            if isinstance(operation, OOperationConnect) or isinstance(operation, OOperationDBOpen):
                self.session_id = parsed_data["session-id"]
                logging.debug("session id {} saved".format(self.session_id))

                if "token" in parsed_data:
                    self.token = parsed_data["token"]
                    logging.debug("token {} saved".format(self.token))
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
        if self.session_id is None:
            head = struct.pack(">b i", operation_type, -1)
        else:
            head = struct.pack(">b i", operation_type, self.session_id)

        if self.token_based:
            if operation_type != OOperationType.REQUEST_CONNECT.value and operation_type != OOperationType.REQUEST_DB_OPEN.value:
                length = len(self.token)
                head += struct.pack(">i {}s".format(length), length, self.token)

        return head

    def parserequest(self, operation: OOperation, data: dict):
        logging.debug("parse request for {} operation".format(operation.__class__))
        parser = OCodec()

        return parser.encode(operation, data)

    def parseresponse(self, operation: OOperation, data):
        logging.debug("parse response for {} operation".format(operation.__class__))

        parser = OCodec()

        if isinstance(operation, OOperationConnect) or isinstance(operation, OOperationDBOpen):
            operation.token_based = False
        else:
            operation.token_based = self.token_based

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

                self.protocol_version = result['protocol_number']
                logging.info("working with protocol version {}".format(self.protocol_version))
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


