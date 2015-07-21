# OPy

This is an alternative Python 3 OrientDB driver implementation based on the latest release of OrientDB. Currently its supporting binary protocol version 31 of OrientDB version 2.1-rc5. It's still under development and written with the aim to provide an object mapping for the graph database part of OrientDB. More information about the usage will follow as soon as possible.

## Structure
This lib splits into two parts:

*	database: contains all the necessary objects and functions for the binary communication with the OrientDB server
*	client: (not yet ready) contains all the code to provide an object mapping to the graph and vice versa

## Usage

The basic usage is as follows:

		# create the client object
		client = OClient(database="FancyGraphDB", user_name="user", user_password="pass", host="localhost", port=2424)
		
		...
		do stuff
		...
		
		# close connection
		client.close()
		
The process behind this call is fairly straight forward:

 	1. create an ODB object
	2. create an OConnection object
	3. open the connection 
	4. open the desired database
	5. do your stuff using the methods of the ODB object
	6. finally close the connection

All this is wrapped up in a proxy object named OClient. So its enough to create an object of OCLient to initialize a connection to the server and the database. Futhermore this object can or must be used to use the orm abilities.

	    
## Examples

All user defined domain objects which should be used with the OrientDB must subclass either the BaseVertex class for and kind of vertex or th BaseEdge class for any kind of edge. Furthermore it's mandantory to implement the persistent_attributes method. This method provide the names of the attributes which will used by the driver to determine the data to persist.

* ###Create

		class TestVertex(BaseVertex):
		    def __init__(self):
		        super().__init__()
		        self.name = None
		        self.type = None
		        self.some = None # will not be persisted
		
		    # this must be implemented to specify the vars which should be persisted
		    def persistentattributes(self):
		        return ['name', 'type']
		        
		class In(BaseEdge):
		    def __init__(self):
		        super().__init__()
		        
		...
		
		# create class of type vertex
		result = client.do(Create(Class(Country, OPlainClass.VERTEX)))
		
		# create class of type edge
		result = client.create(Class(LocatedAt, OPlainClass.EDGE))

		# vertex class
		class TestVertex(BaseVertex):
		    def __init__(self):
		        super().__init__()
		        self.name = None
		        self.type = None
		        self.some = None # will not be persisted
		
		    # this must be implemented to specify the vars which should be persisted
		    def persistentattributes(self):
		        return ['name', 'type']

		vert = TestVertex()
	    vert.name = "Vertigo"
	    vert.type = "Song"
		
		# create a new vertex object and persist it
		result = client.do(Create(Vertex(vert)))
		
		vert1 = TestVertex()
	    vert1.name = "Homer"
	    vert1.type = "Song"
		
		# it's also possible to add a list of vertices
		result = client.do(Create(Vertices([vert, vert1])))

		# edge class
		class TestEdge(BaseEdge):
		    def __init__(self):
		        super().__init__()
		
		# define the vertices
		vertex1 = TestVertex()
		vertex1.setRID(12,0)
		vertex2 = TestVertex()
		vertex2.setRID(13,0)
		
		edge = TestEdge()
		edge.in_vertex = vertex1
	    edge.out_vertex = vertex2
		
		# add the edge to the database
		result = client.do(Create(Edge(edge)))
		
		edge1 = TestEdge()
		edge1.in_vertex = vertex1
	    edge1.out_vertex = vertex2
		
		# it's also possible to add a list of edges
		result = client.do(Create(Edges([edge, edge1])))
		
		# create property
		result = client.do(Create(Property(VertexClass, "propertyname", OBinaryType.STRING)))
		
		result = client.do(Create(Property.withlinkedclass(VertexClass, "propertyname", OBinaryType.EMBEDDEDLIST, "ClassName")))
		
		result = client.do(Create(Property.withlinkedtype(VertexClass, "propertyname", OBinaryType.LINKLIST, OBinaryType.INTEGER)))
		
		# create index
		result = client.do(Create(Index("test").on(VertexClass, ["id", "bla", "hallo"]))
		
		result = client.do(Create(Index("test").on(VertexClass, ["id", "bla", "hallo"]).withmeta("{lala: false}")))
		
		result = client.do(Create(Index("id").on(VertexClass)))
		
		result = client.do(Create(Index("id").on(VertexClass, None, OSQLIndexType.UNIQUE)))
						
* ###Select

		# simplest case
		result = client.do(Select(VertexClass, (), ()))
		
		# with properties
		result = client.do(Select(VertexClass, ["name"], ()))
		
		# with condition(s)
		result = client.do(Select(VertexClass, (), Where(Condition("type").iseq("Song"))))
		
		result = client.do(Select(VertexClass, (), Where(Or(Condition("name").iseq("Vertigo"),Condition("type").iseq("Song"))))))
		
		result = client.do(Select(VertexClass, (), Where(And(Condition("name").iseq("Vertigo"),Condition("type").iseq("Song")))))
		
		result = client.do(Select(VertexClass, (), Where(And(Or(Condition("test").iseq("1"), Condition("test").iseq("2")),Condition("type").iseq("Song")))))
		
		result = client.do(Select(VertexClass, (), Where(Select(VertexClass, (), Where(Condition("a").iseq("a"))))))
		
		result = client.do(Select(VertexClass, [""], OrderBy("a").asc(), Where(Select(VertexClass, (), Where(Condition("a").iseq("a"))))))
		
		result = client.do(Select(Prefixed(VertexClass, 'l'),["l.a", "l.b"], OrderBy.asc("l.a"), Where(Condition("l.a").iseq("b"))))
		
* ###Delete

		# deletes vertices/edges by class
		result = client.do(Delete(Class(VertexClass)))

		result = client.do(Delete(Class(EdgeClass)))

		# deletes vertex by RID
		result = client.do(Delete(Vertex).byRID('#15:5'))
		
		# delete by object
		result = client.do(Delete(vertexobject))

		result = client.do(Delete(edgeobject))
		
		# deletes edge by object
		result = client.do(Delete(Edge).toRID(edgeobject))
		
		# deletes edge by to and from RID
		result = client.do(Delete(Edge).toRID(vertexobject))

        result = client.do(Delete(Edge).fromRID(testedge))

        result = client.do(Delete(Edge).fromRID('#1:2').parse()

        result = client.do(Delete(Edge).toRID('#1:2').parse()

        result = client.do(Delete(Edge).fromRID('#2:3').toRID('#1:2'))

* ###Drop
		
		# drop class
		result = client.do(Drop(Class(VertexClass)))
		
		# drop property
		result = client.do(Drop(Property(VertexClass, "propertyname")))
		
* ###Move
	
		# move class
		result = client.do(Move("#12:2", Class(VertexClass)))
		
		# move cluster
		result = client.do(Move("#12:2", Cluster("testcluster")))

* ###Traverse
	
		# traverse
		result = client.do(Traverse(["#20:0"], ['*'], ()))
		
		# traverse with property
		result = client.do(Traverse(["#20:0"], ['property'], ()))
		
* ###Truncate
		
		# truncate class
		result = client.do(Truncate(Class(VertexClass)))
		
* ###Direct SQL

		# example shortestpath
		result = client.do(Select(None, ['shortestpath(#20:0, #15:0)'],()))

If you need to execute a SQL query without any object relation you can use the following piece of code

		# direct SQL execution
		result = client.exec("select globalProperties from metadata:schema", "")

But be aware that the results serialization depends on the configuration of your OrientDB instance.
	    
## Notes
On serializing a list auf records you have to make sure, that these records have been already written to the database. So its mandantory that theres a rid assigned to each object in the list.
	    
## Implemented

* REQUEST_CONNECT
* REQUEST\_DB_OPEN
* REQUEST\_DB_CREATE
* REQUEST\_DB_EXIST
* REQUEST\_DB_DROP
* REQUEST\_DB_CLOSE
* REQUEST\_DB_SIZE
* REQUEST\_DB_COUNTRECORDS
* REQUEST\_RECORD_LOAD
* REQUEST\_RECORD_CREATE
* REQUEST\_RECORD_UPDATE
* REQUEST\_RECORD_DELETE
* REQUEST\_COMMAND
* REQUEST\_TX_COMMIT
* REQUEST\_DB_RELOAD

## Not yet implemented

* REQUEST\_INDEX_GET
* REQUEST\_INDEX_PUT
* REQUEST\_INDEX_REMOVE
* REQUEST\_RECORD_LOAD_IF_VERSION_NOT_LATEST
* REQUEST\_DATACLUSTER_ADD
* REQUEST\_DATACLUSTER_DROP	
* REQUEST\_DATACLUSTER_COUNT
* REQUEST\_DATACLUSTER_DATARANGE
|[CONTENT|MERGE <JSON>] not implemented






