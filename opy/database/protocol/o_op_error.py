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

import operator

from opy.common.o_db_constants import OConst, OProfileType
from opy.database.o_db_profile_parser import OProfileParser, OElement, OGroup, OCondition
from opy.database.protocol.o_op import OOperation


__author__ = 'daill'

class OOperationError(OOperation):
  def __init__(self):
    super().__init__(-1)

    self.__request_profile_str = ""
    self.__response_profile_str = "[{exception}(1)(exception-class:string)(exception-message:string)]*(0)(serialized-exception:bytes)"

    self.__request_profile = None
    self.__response_profile = None

  def getresponseprofile(self):
    if self.__response_profile == None:
      profile_parser = OProfileParser()
      self.__response_profile = profile_parser.parse(self.getresponsehead() + self.__response_profile_str)

    return self.__response_profile

  def getrequestprofile(self):
    if self.__request_profile == None:
      profile_parser = OProfileParser()
      self.__request_profile = profile_parser.parse(self.__request_profile_str)
    return self.__request_profile


  def decode(self, unpack_data, data):
    """
    No need to check error here, because we're actually in the error handling routine

    :param unpack_data:
    :param data:
    :return: dictionary with data
    """
    data_dict = {}
    rest = data

    def processelement(element: OElement):
      nonlocal rest
      nonlocal data_dict

      if isinstance(element, OGroup):
        # save main state
        main_dict = data_dict
        # create new dict
        main_dict[OConst.EXCEPTION] = list()
        while element.is_repeating:
          data_dict = {}
          main_dict[OConst.EXCEPTION].append(data_dict)
          # need the first element to test if its repeating
          # in case of an error its the static byte 1
          first_element = element.getelements()[0]

          # make sure we've a static byte here as first element
          assert first_element.type == OProfileType.BYTE_STATIC

          # process the elements of this group
          for sub_element in element.getelements():
            rest = processelement(sub_element)

          # check if the first element is again available
          condition = OCondition(operator.__eq__, first_element.value)
          rest, value = unpack_data(first_element.type, rest, condition, name=element.name)

          # if the next static byte is 1 again we have to proceed, otherwise return
          if condition.valid() == False:
            break
        # copy back original data
        data_dict = main_dict
      else:
        # parse the data
        rest, value = unpack_data(element.type, rest, name=element.name)

        if element.type != OProfileType.BYTE_STATIC:
          # put the value into the dict
          data_dict[element.name] = value
      return rest

    def processprofile(elements):
      for element in elements:

        processelement(element)

      return OConst.OK

    status = processprofile(self.getresponseprofile().getelements())

    return data_dict, status

