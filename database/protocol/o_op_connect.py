from database.o_db_constants import OOperationType
from database.o_db_profile_parser import OProfileParser
from database.protocol.o_op import OOperation

__author__ = 'daill'

class OOperationConnect(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_CONNECT)

    self.__request_profile_str = "(driver-name:string)(driver-version:string)(protocol-version:short)(client-id:string)(serialization-impl:string)(user-name:string)(user-password:string)"
    self.__response_profile_str = "(session-id:int)"

    self.__request_profile = None
    self.__response_profile = None

  def getresponseprofile(self):
    if self.__response_profile == None:
      profile_parser = OProfileParser()
      self.__response_profile = profile_parser.parse(self._OOperation__response_head + self.__response_profile_str)

    return self.__response_profile

  def getrequestprofile(self):
    if self.__request_profile == None:
      profile_parser = OProfileParser()
      self.__request_profile = profile_parser.parse(self.__request_profile_str)

    return self.__request_profile
