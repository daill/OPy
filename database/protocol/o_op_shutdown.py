from database.o_db_constants import OOperationType
from database.protocol.o_op import OOperation

__author__ = 'daill'

class ORequestShutdown(OOperation):
  def __init__(self):
    super().__init__(OOperationType.REQUEST_SHUTDOWN)

    self.__request_profile_str = ""
    self.__response_profile_str = "(protocol_number:byte)"

    self.__request_profile = None
    self.__response_profile = None

  def exec(self, connection):
    return super().exec(connection)

  def get_request_profile(self):
    return super().get_request_profile()

  def get_response_profile(self):
    return super().get_response_profile()

