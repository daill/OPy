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
from opy.client.o_db_base import SystemType

__author__ = 'daill'


def escapeclassname(classname:str):
    return "{}".format(classname)

def retrieveclassname(base_class):
    """
    This method decides whether it should use the default classname via magic member or custom method

    :param base_class:
    :return: class name for query
    """
    if base_class:
        if issubclass(base_class, SystemType):
            return base_class.getcustomclassname()

        return base_class.__name__
    return None