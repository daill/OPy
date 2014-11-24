__author__ = 'daill'

class BaseEntity(object):
    def __init__(self):
        pass

class MetaData(object):
    def __init__(self):
        self.version = 0
        self.position = 0
        self.cluster = 0

class BaseVertex(BaseEntity, MetaData):
    """
    Class which has to be implemented by all class who should be persisted within the orientdb
    Its necessary to use public vars in order to provide access to getter and setter

    The edges will be organized in a dict where the key denotes the edge name.
    """
    def __init__(self):
        BaseEntity.__init__(self)
        MetaData.__init__(self)
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
        return '#{}:{}'.format(self.cluster, self.position  )

class BaseEdge(BaseEntity, MetaData):
    """
    You can extend this class to provide own attributes
    Every edge must have an incoming and outgoing edge to a vertex.
    Furthermore the can be use defined properties
    """
    def __init__(self, target:BaseVertex):
        BaseEntity.__init__(self)
        MetaData.__init__(self)
        self.in_vertex = None
        self.out_vertex = target