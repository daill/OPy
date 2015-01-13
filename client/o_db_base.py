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

class BaseVertex(BaseEntity):
    """
    Class which has to be implemented by all class who should be persisted within the orientdb
    Its necessary to use public vars in order to provide access to getter and setter

    The edges will be organized in a dict where the key denotes the edge name.
    """
    def __init__(self):
        super().__init__()
        self.in_edges = None
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

        for edge in self.__out_edges.values():
            edge.in_vertex = self

        return self.__out_edges

    out_edges = property(getoutedges, setoutedges)

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
        self.in_vertex = None
        self.out_vertex = target