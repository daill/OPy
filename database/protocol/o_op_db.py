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

from common.o_db_constants import OOperationType, OConst
from database.o_db_profile_parser import OProfileParser, OElement, OGroup
from database.protocol.o_op import OOperation


__author__ = 'daill'

class OOperationDBClose(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_DB_CLOSE)

    self.__request_profile_str = ""
    self.__response_profile_str = ""

    self.__request_profile = None
    self.__response_profile = None

  def getrequestprofile(self):
    return self.__request_profile

class OOperationDBCountrecords(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_DB_COUNTRECORDS)

    self.__request_profile_str = ""
    self.__response_profile_str = "(count:long)"

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

class OOperationDBSize(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_DB_SIZE)

    self.__request_profile_str = ""
    self.__response_profile_str = "(size:long)"

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

class OOperationDBCreate(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_DB_CREATE)

    self.__request_profile_str = "(database-name:string)(database-type:string)(storage-type:string)"
    self.__response_profile_str = ""

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

class OOperationDBDrop(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_DB_DROP)

    self.__request_profile_str = "(database-name:string)(server-storage-type:string)"
    self.__response_profile_str = ""

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

class OOperationDBExist(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_DB_EXIST)

    self.__request_profile_str = "(database-name:string)(server-storage-type:string)"
    self.__response_profile_str = "(result:byte)"

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

class OOperationDBList(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_DB_LIST)

    self.__request_profile_str = ""
    self.__response_profile_str = "(list:bytes)"

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

class OOperationDBOpen(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_DB_OPEN)

    self.__request_profile_str = "(driver-name:string)(driver-version:string)(protocol-version:short)(client-id:string)(serialization-impl:string)(token-session:boolean)(database-name:string)(database-type:string)(user-name:string)(user-password:string)"
    self.__response_profile_str = "(session-id:int)(token:bytes)(num-of-clusters:short)[(cluster-name:string)(cluster-id:short)](cluster-config:bytes)(orientdb-release:string)"

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
    num_cluster = 0

    def processelement(element: OElement):
      nonlocal rest
      nonlocal data_dict
      nonlocal num_cluster

      if isinstance(element, OGroup):

        # save main state
        main_dict = data_dict

        main_dict[element.name] = list()

        while (num_cluster > 0):
          data_dict = {}
          for sub_element in element.getelements():
            rest = processelement(sub_element)
          num_cluster -= 1
          main_dict[element.name].append(data_dict)

        data_dict = main_dict
      else:
        # handling of a term
        rest, value = unpack_data(element.type, rest, name=element.name)

        if element.name == "num-of-clusters":
          # save value as indicator how often the following group will be repeated
          num_cluster = value

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


class OOperationDBReload(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_DB_RELOAD)

    self.__request_profile_str = ""
    self.__response_profile_str = "(num-of-clusters:short)[{clusters}(cluster-name:string)(cluster-id:short)]"

    self.__request_profile = None
    self.__response_profile = None

  def getresponseprofile(self):
    if self.__response_profile is None:
      profile_parser = OProfileParser()
      self.__response_profile = profile_parser.parse(self.__response_profile_str)

    return self.__response_profile

  def getrequestprofile(self):
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
    num_cluster = 0

    def processelement(element: OElement):
      nonlocal rest
      nonlocal num_cluster
      nonlocal data_dict

      if isinstance(element, OGroup):
        nonlocal num_cluster

        # save main state
        main_dict = data_dict

        main_dict[element.name] = list()

        while (num_cluster > 0):
          data_dict = {}
          for sub_element in element.getelements():
            rest = processelement(sub_element)
          num_cluster -= 1
          main_dict[element.name].append(data_dict)

        data_dict = main_dict
      else:
        # handling of a term
        rest, value = unpack_data(element.type, rest, name=element.name)

        if element.name == "num-of-clusters":
          # save value as indicator how often the following group will be repeated
          num_cluster = value

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