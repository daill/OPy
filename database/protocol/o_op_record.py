import logging
from database.o_db_constants import OOperationType, OConst, ORecordKind
from database.o_db_profile_parser import OProfileParser, OElement, OGroup
from database.protocol.o_op import OOperation

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

    def getresponseprofile(self):
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

class OOperationRecordUpdate(OOperation):
    def __init__(self):
        super().__init__(OOperationType.REQUEST_RECORD_UPDATE)

        self.__request_profile_str = "(cluster-id:short)(cluster-position:long)(update-content:boolean)(record-content:bytes)(record-version:int)(record-type:byte)(mode:byte)"
        self.__response_profile_str = "(record-version:int)(count-of-collection-changes:int)[{changes}(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*"

        self.__request_profile = None
        self.__response_profile = None

    def getresponseprofile(self):
        if self.__response_profile is None:
            profile_parser = OProfileParser()
            self.__response_profile = profile_parser.parse(self.getresponsehead() + self.__response_profile_str)

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
        count_of_collection_changes = 0

        def processelement(element: OElement):
            nonlocal rest

            if isinstance(element, OGroup):
                nonlocal data_dict
                nonlocal count_of_collection_changes

                # save main state
                main_dict = data_dict

                main_dict[element.name] = list()

                while (count_of_collection_changes > 0):
                    data_dict = {}
                    for sub_element in element.getelements():
                        rest = processelement(sub_element)
                    count_of_collection_changes -= 1
                    main_dict[element.name].append(data_dict)

                data_dict = main_dict
            else:
                # handling of a term
                rest, value = unpack_data(element.type, rest, name=element.name)

                if element.name == "count-of-collection-changes":
                    # save value as indicator how often the following group will be repeated
                    nonlocal count_of_collection_changes
                    count_of_collection_changes = value

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

class OOperationRecordDelete(OOperation):
    def __init__(self):
        super().__init__(OOperationType.REQUEST_RECORD_DELETE)

        self.__request_profile_str = "(cluster-id:short)(cluster-position:long)(record-version:int)(mode:byte)"
        self.__response_profile_str = "(payload-status:byte)"

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

class OOperationRecordCreate(OOperation):
    def __init__(self):
        super().__init__(OOperationType.REQUEST_RECORD_CREATE)

        # datasegment id removed since 2.0
        # self.__request_profile_str = "(datasegment-id:int)(cluster-id:short)(record-content:bytes)(record-type:byte)(mode:byte)"
        self.__request_profile_str = "(cluster-id:short)(record-content:bytes)(record-type:byte)(mode:byte)"
        self.__response_profile_str = "(cluster-position:long)(record-version:int)(count-of-collection-changes:int)[{update-info}(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]"

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
        count_of_collection_changes = 0

        def processelement(element: OElement):
            nonlocal rest

            if isinstance(element, OGroup):
                nonlocal data_dict
                nonlocal count_of_collection_changes

                # save main state
                main_dict = data_dict

                main_dict[element.name] = list()

                while (count_of_collection_changes > 0):
                    data_dict = {}
                    for sub_element in element.getelements():
                        rest = processelement(sub_element)
                    count_of_collection_changes -= 1
                    main_dict[element.name].append(data_dict)

                data_dict = main_dict
            else:
                # handling of a term
                rest, value = unpack_data(element.type, rest, name=element.name)

                if element.name == "count-of-collection-changes":
                    # save value as indicator how often the following group will be repeated
                    nonlocal count_of_collection_changes
                    count_of_collection_changes = value

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


class OOperationRecordLoad(OOperation):
    def __init__(self):
        super().__init__(OOperationType.REQUEST_RECORD_LOAD)

        self.__request_profile_str = "(cluster-id:short)(cluster-position:long)(fetch-plan:string)(ignore-cache:byte)(load-tombstones:byte)"
        self.__response_profile_str = "[{payload}(payload-status:byte)[{records}(record-content:bytes)(record-version:int)(record-type:byte)]*]+"

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

        def processelement(element: OElement):
            nonlocal rest

            if isinstance(element, OGroup):
                nonlocal data_dict

                # save main state
                main_dict = data_dict

                main_dict[element.name] = list()

                while element.is_repeating:
                    data_dict = {}

                    for sub_element in element.getelements():
                        rest = processelement(sub_element)

                    main_dict[element.name].append(data_dict)
                    break

                data_dict = main_dict
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
                    return OConst.ERROR.value

                processelement(element)

            return OConst.OK


        status = processprofile(self.getresponseprofile().getelements())

        # return the status (OK|Error) to decide what to do next and the extracted data
        return data_dict, status
