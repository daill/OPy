import logging
from database.o_db_constants import OOperationType, OConst
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

class OOperationRequestConfigSet(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_CONFIG_SET)

    self.__request_profile_str = "(key:string)(value:string)"
    self.__response_profile_str = ""

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

class OOperationRequestConfigList(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_CONFIG_LIST)

    self.__request_profile_str = ""
    self.__response_profile_str = "(num-cfg-items:short)[(config-key:string)(config-value:string)]"

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
    num_cfg_items = 0

    def process_element(element: OElement):
      nonlocal rest

      if isinstance(element, OGroup):
        nonlocal data_dict
        nonlocal num_cfg_items

        # save main state
        main_dict = data_dict

        main_dict[element.get_name()] = list()

        while (num_cfg_items > 0):
          data_dict = {}
          for sub_element in element.get_elements():
            rest = process_element(sub_element)
          num_cfg_items -= 1
          main_dict[element.get_name()].append(data_dict)

        data_dict = main_dict
      else:
        # handling of a term
        rest, value = unpack_data(element.type, rest)

        if element.name == "num-cfg-items":
          # save value as indicator how often the following group will be repeated
          nonlocal num_cfg_items
          num_cfg_items = value

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