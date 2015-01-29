# OPy


This is an alternative Python 3 OrientDB driver implementation based on the latest release of OrientDB. Currently its supporting binary protocol version 28 of OrientDB version 2.0. It's still under development and written with the aim to provide an object mapping for the graph database part of OrientDB. More information about the usage will follow as soon as possible.

## Structure
This tool splits into two parts:

*	database: contains all the necessary objects and functions for the binary communication with the OrientDB server
*	client: (not yet ready) contains all the code to provide an object mapping to the graph and vice versa

## Usage

The basic usage structur is as follows:

1. create an ODB object
2. create an OConnection object
3. open the connection 
4. open the desired database
5. do your stuff using the methods of the ODB object
6. finally close the connection

		# create ODB object
		odb = ODB()
		# create OConnection object
	    connection = OConnection()
	    # connect to server
    	odb.connect(connection, user_name="user", user_password="pass")
		# connect to database
	    odb.dbopen(connection, database_name="FanceGraphDB", 	database_type=ODBType.DOCUMENT.value, user_name="user", user_password="pass")
	    
	    ... 
	    do stuff 
	    ...
	    
	    # close connection
	    odb.dbclose(connection)
	    
## Implemented

* REQUEST_CONNECT
* REQUEST_DB_OPEN
* REQUEST_DB_CREATE
* REQUEST_DB_EXIST
* REQUEST_DB_DROP
* REQUEST_DB_CLOSE
* REQUEST_DB_SIZE
* REQUEST_DB_COUNTRECORDS
* REQUEST_RECORD_LOAD
* REQUEST_RECORD_CREATE
* REQUEST_RECORD_UPDATE
* REQUEST_RECORD_DELETE
* REQUEST_COMMAND
* REQUEST_TX_COMMIT
* REQUEST_DB_RELOAD

## Not implemented
* REQUEST_DATACLUSTER_ADD
* REQUEST_DATACLUSTER_DROP	
* REQUEST_DATACLUSTER_COUNT
* REQUEST_DATACLUSTER_DATARANGE




