import socket
import logging
import struct
from enum import Enum
import sys


__author__ = 'daill'

class ORecord(object):
  """
  Represents a record in the context of OrientDB.
  It starts with a short where:
  -2 => null record
  -3 => RID
  0 => record

  TODO: implement
  """

  def __init__(self, cluster_id, ):
    super().__init__()

