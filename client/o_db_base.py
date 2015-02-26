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

__author__ = 'daill'

class BaseEntity(object):
    def __init__(self):
        self.version = None
        self.clusterposition = None
        self.clusterid = None

    def getRID(self):
        return "#{}:{}".format(self.clusterid, self.clusterposition)

    def setRID(self, clusterid:int, clusterposition:int):
        self.clusterid = clusterid
        self.clusterposition = clusterposition

    def __eq__(self, other):
        return (self.clusterposition == other.clusterposition) and (self.clusterid == other.clusterid)

class BaseVertex(BaseEntity):
    """
    Class which has to be implemented by all class who should be persisted within the orientdb
    Its necessary to use public vars in order to provide access to getter and setter

    The edges will be organized in a dict where the key denotes the edge name.
    """
    def __init__(self):
        super().__init__()
        self.__in_edges = None
        self.__out_edges = None

    def persistent_attributes(self):
        """
        This method should the names of the fields which should be saved by persistence framework to the orient db instance
        :return: List of field names
        """
        raise NotImplementedError("You have to implement this method to ensure that the correct values will be saved")

    def getoutedges(self):
        return self.__out_edges


    def setoutedges(self, edges: dict):
        self.__out_edges = edges

        for edge_list in self.__out_edges.values():
            for edge in edge_list:
                edge.in_vertex = self

        return self.__out_edges

    def getinedges(self):
        return self.__in_edges


    def setinedges(self, edges: dict):
        self.__in_edges = edges

        for edge_list in self.__in_edges.values():
            for edge in edge_list:
                edge.out_vertex = self

        return self.__out_edges

    out_edges = property(getoutedges, setoutedges)
    in_edges = property(getinedges, setinedges)

    def getrid(self):
        return '#{}:{}'.format(self.clusterid, self.clusterposition  )

class BaseEdge(BaseEntity):
    """
    You can extend this class to provide own attributes
    Every edge must have an incoming and outgoing edge to a vertex.
    Furthermore the can be use defined properties
    """
    def __init__(self, target:BaseVertex=None):
        super().__init__()
        self.tmp_rid = None
        self.in_vertex = None
        self.out_vertex = target