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

__author__ = 'daill'

from distutils.core import setup
setup(
    # Application name:
    name="opy",

    # Version number (initial):
    version="0.5.1",

    # Application author details:
    author="Christian Kramer",
    author_email="ck.daill@gmail.com",

    # Packages
    packages=["opy",
              "opy.client",
              "opy.common",
              "opy.database",
              "opy.database.protocol",
              "opy.test",
              "opy.test.model",
              "opy.tools"],

    # Details
    url="https://github.com/daill/OPy",

    license="LICENSE.txt",
    description="Native binary OrientDB driver.",
)

