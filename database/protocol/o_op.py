import logging
from database.o_db_exceptions import ProfileNotMatchException
from database.o_db_constants import OConst, OOperationType, ORecordKind
from database.o_db_profile_parser import OElement, OGroup, OProfileParser

__author__ = 'daill'

class ORecord(object):
    """
    This class represents a record.

    -3 for RID
    -2 for null record
    Everything else for a record
    """
    def __init__(self, kind:ORecordKind):
        self.__kind = kind

        self.__rid_profile_str = "(cluster-id:short)(cluster-position:long)"
        self.__record_profile_str = "(record-type:byte)(cluster-id:short)(cluster-position:long)(record-version:int)(record-content:bytes)"
        self.__record_profile = None
        self.__rid_profile = None

    def get_response_profile(self):
        profile_parser = OProfileParser()

        if self.__kind == ORecordKind.NULL:
            # do nothing return empty profile
            return profile_parser.parse("")
        elif self.__kind == ORecordKind.RID:
            if self.__rid_profile is None:
                self.__rid_profile = profile_parser.parse(self.__rid_profile_str)
            return self.__rid_profile
        else:
            if self.__record_profile is None:
                self.__record_profile = profile_parser.parse(self.__record_profile_str)
            return self.__record_profile


class OOperation(object):
    def __init__(self, operation_type:OOperationType):
        self.__operation_type = operation_type
        self.__response_head = "(success_status:byte)(session-id:int)"

    def exec(self, connection):
        raise NotImplementedError("You have to implement the exec method")

    def get_operation_type(self):
        return self.__operation_type.value

    def get_request_profile(self):
        raise NotImplementedError("You have to implement a method to return the built request profile")

    def get_response_profile(self):
        raise NotImplementedError("You have to implement a method to return the built response profile")

    def decode(self, unpack_data, data):
        """
        Decodes the given bytes to the appropriate response profile. If there are some special handlings in a
        specific profile we can implement this method within each operation separately.

        :param unpack_data:
        :param data:
        :return:
        """
        data_dict = {}
        error_state = False
        rest = data

        def process_element(element: OElement):
            nonlocal rest

            if isinstance(element, OGroup):
                # standard handling of group with non repeating elements
                for sub_element in element.get_elements():
                    rest = process_element(sub_element)
            else:
                # handling of a term
                # check if there are bytes left
                if not rest or len(rest) == 0:
                    return

                rest, value = unpack_data(element.type, rest, name=element.name)

                # check if its and error
                if element.name == OConst.SUCCESS_STATUS.value and value == 1:
                    logging.error("received an error from the server. start handling")
                    nonlocal error_state
                    error_state = True
                    return

                nonlocal data_dict
                data_dict[element.name] = value
            return rest

        def process_profile(elements):
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

                process_element(element)

            return OConst.OK


        status = process_profile(self.get_response_profile().get_elements())

        # return the status (OK|Error) to decide what to do next and the extracted data
        return data_dict, status

    def encode(self, pack_data, arguments):

        def process_element(element: OElement):
            if isinstance(element, OGroup):
                return process_element(element)
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

        def process_profile(elements):
            result = b''
            for element in elements:
                result += process_element(element)

            return result

        if self.get_request_profile() is not None:
            return process_profile(self.get_request_profile().get_elements())

        return b''
