import logging
from database.o_db_constants import OOperationType, OConst
from database.o_db_profile_parser import OProfileParser, OElement, OGroup
from database.protocol.o_op import OOperation

__author__ = 'daill'

class OOperationRecordCreate(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_RECORD_CREATE)

    self.__request_profile_str = "(datasegment-id:int)(cluster-id:short)(record-content:bytes)(record-type:byte)(mode:byte)"
    self.__response_profile_str = "(cluster-position:long)(record-version:int)(count-of-collection-changes:int)[{update-info}(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]"

    self.__request_profile = None
    self.__response_profile = None

  def get_response_profile(self):
    if self.__response_profile is None:
      profile_parser = OProfileParser()
      self.__response_profile = profile_parser.parse(self._OOperation__response_head + self.__response_profile_str)

    return self.__response_profile

  def get_request_profile(self):
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

    def process_element(element: OElement):
      nonlocal rest

      if isinstance(element, OGroup):
        nonlocal data_dict
        nonlocal count_of_collection_changes

        # save main state
        main_dict = data_dict

        main_dict[element.get_name()] = list()

        while (count_of_collection_changes > 0):
          data_dict = {}
          for sub_element in element.get_elements():
            rest = process_element(sub_element)
          count_of_collection_changes -= 1
          main_dict[element.get_name()].append(data_dict)

        data_dict = main_dict
      else:
        # handling of a term
        rest, value = unpack_data(element.type, rest)

        if element.name == "count-of-collection-changes":
          # save value as indicator how often the following group will be repeated
          nonlocal count_of_collection_changes
          count_of_collection_changes = value

        # check if its and error
        if element.name == OConst.SUCCESS_STATUS and value == 1:
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

class OOperationRecordLoad(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_RECORD_LOAD)

    self.__request_profile_str = "(cluster-id:short)(cluster-position:long)(fetch-plan:string)(ignore-cache:byte)(load-tombstones:byte)"
    self.__response_profile_str = "[{payload}(payload-status:byte)[{records}(record-content:bytes)(record-version:int)(record-type:byte)]*]+"

    self.__request_profile = None
    self.__response_profile = None

  def get_response_profile(self):
    if self.__response_profile is None:
      profile_parser = OProfileParser()
      self.__response_profile = profile_parser.parse(self._OOperation__response_head + self.__response_profile_str)

    return self.__response_profile

  def get_request_profile(self):
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

    def process_element(element: OElement):
      nonlocal rest

      if isinstance(element, OGroup):
        nonlocal data_dict

        # save main state
        main_dict = data_dict

        main_dict[element.get_name()] = list()

        while element.is_repeating:
          data_dict = {}

          for sub_element in element.get_elements():
            rest = process_element(sub_element)

          main_dict[element.get_name()].append(data_dict)
          break

        data_dict = main_dict
      else:
        # handling of a term
        rest, value = unpack_data(element.type, rest)

        # check if its and error
        if element.name == OConst.SUCCESS_STATUS and value == 1:
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