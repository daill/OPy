from database.o_db_constants import OOperationType
from database.o_db_profile_parser import OProfileParser
from database.protocol.o_op import OOperation

__author__ = 'daill'


class OOperationInit(OOperation):
  def __init__(self):
    super().__init__(0)

    self.__request_profile_str = ""
    self.__response_profile_str = "(protocol_number:short)"

    self.__request_profile = None
    self.__response_profile = None

  def getresponseprofile(self):
    if self.__response_profile is None:
      profile_parser = OProfileParser()
      self.__response_profile = profile_parser.parse(self.__response_profile_str)

    return self.__response_profile

  def getrequestprofile(self):
    if self.__request_profile is None:
      profile_parser = OProfileParser()
      self.__request_profile = profile_parser.parse(self.__request_profile_str)
    return self.__request_profile










