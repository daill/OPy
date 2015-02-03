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

from common.o_db_model import OSQLPayload, ORecord

from database.o_db_constants import OOperationType, OConst, OProfileType, ORecordKind
from common.o_db_exceptions import ProfileNotMatchException
from database.o_db_profile_parser import OProfileParser, OElement, OGroup
from database.protocol.o_op import OOperation


__author__ = 'daill'


class OOperationRequestConfigGet(OOperation):
    def __init__(self):
        super().__init__(OOperationType.REQUEST_CONFIG_GET)

        self.__request_profile_str = "(key:string)"
        self.__response_profile_str = "(value:string)"

        self.__request_profile = None
        self.__response_profile = None

    def getresponseprofile(self):
        if self.__response_profile is None:
            profile_parser = OProfileParser()
            self.__response_profile = profile_parser.parse(
                self.getresponsehead() + self.__response_profile_str)

        return self.__response_profile

    def getrequestprofile(self):
        if self.__request_profile is None:
            profile_parser = OProfileParser()
            self.__request_profile = profile_parser.parse(self.__request_profile_str)
        return self.__request_profile


class OOperationRequestConfigSet(OOperation):
    def __init__(self):
        super().__init__(OOperationType.REQUEST_CONFIG_SET)

        self.__request_profile_str = "(key:string)(value:string)"
        self.__response_profile_str = ""

        self.__request_profile = None
        self.__response_profile = None

    def getresponseprofile(self):
        if self.__response_profile is None:
            profile_parser = OProfileParser()
            self.__response_profile = profile_parser.parse(
                self.getresponsehead() + self.__response_profile_str)

        return self.__response_profile

    def getrequestprofile(self):
        if self.__request_profile is None:
            profile_parser = OProfileParser()
            self.__request_profile = profile_parser.parse(self.__request_profile_str)
        return self.__request_profile


class OOperationRequestConfigList(OOperation):
    def __init__(self):
        super().__init__(OOperationType.REQUEST_CONFIG_LIST)

        self.__request_profile_str = ""
        self.__response_profile_str = "(num-cfg-items:short)[(config-key:string)(config-value:string)]"

        self.__request_profile = None
        self.__response_profile = None

    def getresponseprofile(self):
        if self.__response_profile is None:
            profile_parser = OProfileParser()
            self.__response_profile = profile_parser.parse(
                self.getresponsehead() + self.__response_profile_str)

        return self.__response_profile

    def getrequestprofile(self):
        if self.__request_profile is None:
            profile_parser = OProfileParser()
            self.__request_profile = profile_parser.parse(self.__request_profile_str)
        return self.__request_profile

    def decode(self, unpack_data, data):
        """
        Need to override because of the dependencies of term and group

        :param unpack_data:
        :param data:
        :return:
        """
        data_dict = {}
        error_state = False
        rest = data
        num_cfg_items = 0

        def processelement(element: OElement):
            nonlocal rest, num_cfg_items, data_dict

            if isinstance(element, OGroup):

                # save main state
                main_dict = data_dict

                main_dict[element.name] = list()

                while (num_cfg_items > 0):
                    data_dict = {}
                    for sub_element in element.getelements():
                        rest = processelement(sub_element)
                    num_cfg_items -= 1
                    main_dict[element.name].append(data_dict)

                data_dict = main_dict
            else:
                # handling of a term
                rest, value = unpack_data(element.type, rest, name=element.name)

                if element.name == "num-cfg-items":
                    # save value as indicator how often the following group will be repeated
                    num_cfg_items = value

                # check if its and error
                if element.name == OConst.SUCCESS_STATUS.value and value == 1:
                    logging.error("received an error from the server. start handling")
                    nonlocal error_state
                    error_state = True
                    return

                data_dict[element.name] = value
            return rest

        def processprofile(elements):
            """
            Iterate of the whole set of profile elements and unpack them
            :param elements:
            :return:
            """
            for element in elements:
                # fetch an error if it occurs
                nonlocal error_state
                if error_state:
                    return OConst.ERROR

                processelement(element)

            return OConst.OK


        status = processprofile(self.getresponseprofile().getelements())

        # return the status (OK|Error) to decide what to do next and the extracted data
        return data_dict, status


class OOperationRequestCommand(OOperation):
    """
    This request uses different types of request. In case of a query we have to use the shortcut 'q' as class-name. In case of a
    non-idempotent coammand like update or insert
    """
    def __init__(self, command_payload:OSQLPayload, protocol_version:int):
        super().__init__(OOperationType.REQUEST_COMMAND)

        self.__request_profile_str = "(mode:byte)(command-payload-length:int)(class-name:string)"
        self.__response_profile_str_sync = "[{result}(synch-result-type:byte)[{records}(synch-result-content:record)]]+"
        self.__response_profile_str_async = "[{result}(asynch-result-type:byte)[{records}(asynch-result-content:record)]*](pre-fetched-record-size.md)[(pre-fetched-record)]*+"

        self.__request_profile = None
        self.__response_profile = None
        self.__protocol_version = protocol_version

        self.__async = False

        self.__command_payload = command_payload

    def setasync(self, async:bool):
        self.__async = async

    def getresponseprofile(self):
        if self.__response_profile is None:
            profile_parser = OProfileParser()
            if self.__async:
                self.__response_profile = profile_parser.parse(self.getresponsehead() + self.__response_profile_str_async)
            else:
                self.__response_profile = profile_parser.parse(self.getresponsehead() + self.__response_profile_str_sync)
        return self.__response_profile

    def getrequestprofile(self):
        if self.__request_profile is None:
            profile_parser = OProfileParser()
            self.__request_profile = profile_parser.parse(self.__request_profile_str + self.__command_payload.getprofilestr())
        return self.__request_profile


    def encode(self, pack_data, arguments):
        """
        Needs to overwrite this in case of special handling

        :param pack_data:
        :param arguments:
        :return:
        """
        command_payload_length_name = "command-payload-length"

        def processelement(element: OElement):
            if element.name == command_payload_length_name:
                return b''

            if isinstance(element, OGroup):
                return processelement(element)
            else:
                if element.name in arguments:
                    _result = b''
                    if element.is_repeating:
                        for arg_data in arguments[element.name]:
                            _result += pack_data(element.type, arg_data, name=element.name)
                    else:
                        _result += pack_data(element.type, arguments[element.name], name=element.name)
                    return _result
                else:
                    raise ProfileNotMatchException(
                        "argument {} could not be found in argument data".format(element.name))

        def processprofile(elements):
            result = b''
            process_counter = 0
            orig_result = None


            for element in elements:
                if element.name == command_payload_length_name:
                    orig_result = result
                    result = b''
                    continue
                else:
                    result += processelement(element)

                process_counter += 1

                if process_counter == (len(elements)-1):
                    size = len(result)
                    command_payload_length = pack_data(type=OProfileType.INT, value=size, name=command_payload_length_name)
                    result = orig_result + command_payload_length + result

            return result

        if self.getrequestprofile() is not None:
            return processprofile(self.getrequestprofile().getelements())

        return b''


    def decode(self, unpack_data, data):
        """
        Need to override because of the dependencies of term and group

        :param unpack_data:
        :param data:
        :return:
        """
        data_dict = {}
        error_state = False
        synch_result_type = None

        def parserecord(main_dict:dict, rest:bytes, name:str):
            """
            Reads a record by its definition

            :param main_dict:
            :param rest:
            :param name:
            :return: returns rest bytes
            """
            nonlocal data_dict
            data_dict = {}
            rest, value = unpack_data(OProfileType.SHORT, rest, name="record-kind")
            elements = ORecord(ORecordKind(value)).getresponseprofile().getelements()
            # parse each record
            for sub_element in elements:
                rest = processelement(sub_element, rest)
            main_dict[name].append(data_dict)
            return rest

        def processelement(element: OElement, rest:bytes):
            """
            Parses the bytes base on the element definition

            :param element:
            :param rest:
            :return: returns rest bytes
            """
            nonlocal data_dict
            nonlocal synch_result_type

            if isinstance(element, OGroup):

                # save main state
                main_dict = data_dict

                main_dict[element.name] = list()

                if element.name and element.name == "records":

                    if synch_result_type == 'n':
                        logging.debug("parsing null record command response")
                        # null record
                        # do nothing
                        pass
                    elif synch_result_type == 'r':
                        logging.debug("parsing single record command response")
                        # single record
                        rest = parserecord(main_dict, rest, element.name)
                    elif synch_result_type == 'l':
                        logging.debug("parsing record list command response")
                        # list of records
                        rest, count = unpack_data(OProfileType.INT, rest, name="count")

                        for i in range(count):
                            rest = parserecord(main_dict, rest, element.name)

                    elif synch_result_type == 'a':
                        logging.debug("parsing serialized records command response")

                        rest, type = unpack_data(OProfileType.STRING, rest, name="type")

                        # serialized result
                        # TODO implement
                        pass

                    if self.__protocol_version > 17:
                        logging.debug("using new version of command response parsing")
                        while len(rest) > 0:
                            rest, status = unpack_data(OProfileType.BYTE, rest, name="status")
                            if status > 0:
                                # read record
                                length = len(main_dict)
                                rest = parserecord(main_dict, rest, element.name)

                                if len(main_dict) > length and status == 2:
                                    # do something
                                    pass

                else:
                    while True:
                        if len(rest) <= 1:
                            break
                        data_dict = {}
                        for sub_element in element.getelements():
                            rest = processelement(sub_element, rest)
                        main_dict[element.name].append(data_dict)

                data_dict = main_dict
            else:
                # handling of a term
                rest, value = unpack_data(element.type, rest, name=element.name)

                if element.name == "synch-result-type":
                    if value != 2:
                        synch_result_type = chr(value)
                    else:
                        synch_result_type = value


                # handle record
                if element.type == OProfileType.RECORD:
                    record = ORecord(value)

                    # save main state
                    main_dict = data_dict
                    data_dict = {}

                    processprofile(record.getresponseprofile())

                    main_dict[element.get_name()].append(data_dict)
                    data_dict = main_dict


                # check if its and error
                if element.name == OConst.SUCCESS_STATUS.value and value == 1:
                    logging.error("received an error from the server. start handling")
                    nonlocal error_state
                    error_state = True
                    return

                # write value to dict
                data_dict[element.name] = value
            return rest

        def processprofile(elements, data:bytes):
            """
            Iterate of the whole set of profile elements and unpack them
            :param elements:
            :return:
            """
            for element in elements:
                # fetch an error if it occurs
                nonlocal error_state
                if error_state:
                    return OConst.ERROR

                data = processelement(element, data)

            return OConst.OK


        status = processprofile(self.getresponseprofile().getelements(), data)

        # return the status (OK|Error) to decide what to do next and the extracted data
        return data_dict, status


class OOperationRequestTXCommit(OOperation):
    def __init__(self, entries_profile:str):
        super().__init__(OOperationType.REQUEST_TX_COMMIT)

        self.__entries_profile = entries_profile

        self.__request_profile_str = "(tx-id:int)(using-tx-log:byte)"
        self.__response_profile_str = "(created-record-count:int)[{record-created}(client-specified-cluster-id:short)(client-specified-cluster-position:long)(created-cluster-id:short)(created-cluster-position:long)]*(updated-record-count:int)[{record-updated}(updated-cluster-id:short)(updated-cluster-position:long)(new-record-version:int)]*(count-of-collection-changes:int)[{records-canged}(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*"

        self.__request_profile = None
        self.__response_profile = None

    def getresponseprofile(self):
        if self.__response_profile is None:
            profile_parser = OProfileParser()
            self.__response_profile = profile_parser.parse(self.getresponsehead() + self.__response_profile_str)

        return self.__response_profile

    def getrequestprofile(self):
        """
        This method needs to handle the profile in a special way because of alternating entry profiles
        :return:
        """
        if self.__request_profile is None:
            profile_parser = OProfileParser()
            profile_to_parse = self.__request_profile_str

            for entry_profile in self.__entries_profile:
                profile_to_parse += "[{entry}(begin:byte)" + entry_profile + "]"

            profile_to_parse += "(end:byte)(remote-index-length:string)"
            self.__request_profile_str = profile_to_parse
            self.__request_profile = profile_parser.parse(self.__request_profile_str)

        return self.__request_profile

    def decode(self, unpack_data, data):
        """
        Need to override because of the dependencies of term and group

        :param unpack_data:
        :param data:
        :return:
        """
        data_dict = {}
        error_state = False
        rest = data
        num_repeats = 0

        def processelement(element: OElement):
            nonlocal rest, data_dict

            if isinstance(element, OGroup):
                nonlocal num_repeats

                # save main state
                main_dict = data_dict

                main_dict[element.name] = list()

                while (num_repeats > 0):
                    data_dict = {}
                    for sub_element in element.getelements():
                        rest = processelement(sub_element)
                    num_repeats -= 1
                    main_dict[element.name].append(data_dict)

                data_dict = main_dict
            else:
                # handling of a term
                rest, value = unpack_data(element.type, rest, name=element.name)

                if element.name == "created-record-count" or element.name == "updated-record-count" or element.name == "count-of-collection-changes":
                    num_repeats = value

                # check if its and error
                if element.name == OConst.SUCCESS_STATUS.value and value == 1:
                    logging.error("received an error from the server. start handling")
                    nonlocal error_state
                    error_state = True
                    return

                data_dict[element.name] = value
            return rest

        def processprofile(elements):
            """
            Iterate of the whole set of profile elements and unpack them
            :param elements:
            :return:
            """
            for element in elements:
                # fetch an error if it occurs
                nonlocal error_state
                if error_state:
                    return OConst.ERROR

                processelement(element)

            return OConst.OK


        status = processprofile(self.getresponseprofile().getelements())

        # return the status (OK|Error) to decide what to do next and the extracted data
        return data_dict, status


    def encode(self, pack_data, arguments):
        def processelement(element: OElement):
            nonlocal arguments

            if isinstance(element, OGroup):
                _result = b''
                temp_arguments = arguments

                if 'entries' not in arguments:
                    raise ProfileNotMatchException("argument {} could not be found in argument data".format('entries'))

                arguments = arguments['entries'].pop(0)

                for _element in element.getelements():
                    _result += processelement(_element)

                arguments = temp_arguments

                return _result
            else:
                if element.name in arguments:
                    _result = b''
                    if element.is_repeating:
                        for arg_data in arguments[element.name]:
                            _result += pack_data(element.type, arg_data, name=element.name)
                    else:
                        _result += pack_data(element.type, arguments[element.name], name=element.name)
                    return _result
                else:
                    raise ProfileNotMatchException("argument {} could not be found in argument data".format(element.name))

        def processprofile(elements):
            result = b''
            for element in elements:
                result += processelement(element)

            return result

        if self.getrequestprofile() is not None:
            return processprofile(self.getrequestprofile().getelements())

        return b''


class OOPerationRequestRidBagGetSize(OOperation):
    """
    collectionPointer = (fileId:long)(pageIndex:long)(pageOffset:int)
    collectionChanges = (changesSize:int)[(link:rid)(changeType:byte)(value:int)]*
    """
    def __init__(self):
        super().__init__(OOperationType.REQUEST_RIDBAG_GET_SIZE)

        self.__request_profile_str = "(fileId:long)(pageIndex:long)(pageOffset:int)(changesSize:int)[(cluster-id:short)(cluster-position:long)(changeType:byte)(value:int)]*"
        self.__response_profile_str = "(size:int)"

        self.__request_profile = None
        self.__response_profile = None