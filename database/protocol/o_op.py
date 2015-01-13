import logging

from common.o_db_exceptions import ProfileNotMatchException
from database.o_db_constants import OConst, OOperationType
from database.o_db_profile_parser import OElement, OGroup


__author__ = 'daill'

class OOperation(object):
    """
    Base class which has to be derived by all other commands
    """
    def __init__(self, operation_type:OOperationType):
        self.__operation_type = operation_type
        self.__response_head = "(success_status:byte)(session-id:int)"
        self.__token = "(token:bytes)"
        self.token_based = False
        self.token = None

    def getresponsehead(self):
        if self.token_based:
            return "{}{}".format(self.__response_head, self.__token)
        else:
            return self.__response_head

    def exec(self, connection):
        raise NotImplementedError("You have to implement the exec method")

    def getoperationtype(self):
        return self.__operation_type.value

    def getrequestprofile(self):
        raise NotImplementedError("You have to implement a method to return the built request profile")

    def getresponseprofile(self):
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

        def processelement(element: OElement):
            nonlocal rest

            if isinstance(element, OGroup):
                # standard handling of group with non repeating elements
                for sub_element in element.getelements():
                    rest = processelement(sub_element)
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
            for element in elements:
                result += processelement(element)

            return result

        if self.getrequestprofile() is not None:
            return processprofile(self.getrequestprofile().getelements())

        return b''