import logging

from database.o_db_connection import OConnection
from database.o_db_constants import OStorageTypes, ODBType, ODriver, OModeInt, ORecordType, OModeChar, OCommandClass, \
    OSerialization
from database.protocol.o_op_connect import OOperationConnect
from database.protocol.o_op_db import OOperationDBClose, OOperationDBCreate, OOperationDBExist, OOperationDBList, \
    OOperationDBOpen, OOperationDBReload, \
    OOperationDBSize, OOperationDBCountrecords
from database.protocol.o_op_record import OOperationRecordCreate, OOperationRecordLoad, OOperationRecordUpdate, \
    OOperationRecordDelete
from database.protocol.o_op_request import OOperationRequestConfigGet, OOperationRequestConfigList, \
    OOperationRequestConfigSet, OOperationRequestCommand, OSQLPayload, OOperationRequestTXCommit


__author__ = 'daill'


class ODB:
    """
    This is the interface to the orient db
    """
    def __init__(self):
        pass

    def dbreload(self, connection:OConnection):
        """
        Reloads database information. Response is the clusternames and ids

        :param connection:
        :return:
        """
        try:

            operation = OOperationDBReload()

            logging.debug("called {}".format(operation))

            response = connection.exec(operation, {})

            return response
        except Exception as err:
            logging.error(err)

    def dbcreate(self, connection:OConnection, database_name:str, database_type:ODBType, storage_type:OStorageTypes):
        """
        Creates a new database.
        This operation is only working when the server_connect function has been run preivously.

        :param connection:
        :param database_name:
        :param database_type:
        :param storage_type:
        :return:
        """
        try:

            operation = OOperationDBCreate()

            # create data for initial connect
            data = {"database-name": database_name,
                    "database-type": database_type,
                    "storage-type": storage_type}

            response = connection.exec(operation, data)

            return response
        except Exception as err:
            logging.error(err)

    def connect(self, connection:OConnection, user_name:str, user_password:str):
        """
        Connects to the OrientDB server to execute command. If you want to interact with the database use the db_open function.

        :param connection:
        :param user_name:
        :param user_password:
        :return:
        """
        try:
            operation_connect = OOperationConnect()

            # create data for initial connect
            data = {"driver-name": ODriver.DRIVER_NAME.value,
                    "driver-version": ODriver.DRIVER_VERSION.value,
                    "protocol-version": connection.getprotocolversion(),
                    "client-id": '-1',
                    "user-name": user_name,
                    "serialization-impl": OSerialization.SERIALIZATION_CSV.value,
                    "user-password": user_password}

            result_data = connection.exec(operation_connect, data)

            return result_data
        except Exception as err:
            logging.error(err)

    def dbopen(self, connection:OConnection, database_name:str, database_type:ODBType, user_name:str,
                user_password:str):
        """
        Opens the connection to a db.
        This operation is only working when the server_connect function has been run preivously.

        :param connection:
        :param database_name:
        :param server_storage_type:
        :return:
        """
        try:

            # create data for initial connect
            request_data = {"driver-name": ODriver.DRIVER_NAME.value,
                            "driver-version": ODriver.DRIVER_VERSION.value,
                            "protocol-version": connection.getprotocolversion(),
                            "client-id": "",
                            "user-name": user_name,
                            "user-password": user_password,
                            "database-name": database_name,
                            "serialization-impl": OSerialization.SERIALIZATION_CSV.value,
                            "database-type": database_type}

            operation = OOperationDBOpen()

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            return response
        except Exception as err:
            logging.error(err)

    def dbclose(self, connection:OConnection):
        """
        Closes the connection to the database and lets the server close the socket

        :param connection:
        :return:
        """
        try:

            operation = OOperationDBClose()

            logging.debug("called {}".format(operation))

            connection.exec(operation, {})

            connection.close()

            return None
        except Exception as err:
            logging.error(err)

    def configset(self, connection:OConnection, key:str, value:str):
        """
        Sets a configuration value

        :param connection:
        :param key:
        :param value:
        :return:
        """
        try:
            operation = OOperationRequestConfigSet()

            # data dict
            data = {"key": key,
                    "value": value}

            logging.debug("called {} with data {}".format(operation, data))

            response = connection.exec(operation, data)

            return response
        except Exception as err:
            logging.error(err)

    def dblist(self, connection:OConnection):
        """
        Retrieves a list of databases from the server

        :param connection:
        :return:
        """
        try:
            operation = OOperationDBList()

            logging.debug("called {}".format(operation))

            response = connection.exec(operation, {})

            return response
        except Exception as err:
            logging.error(err)

    def configlist(self, connection:OConnection):
        """
        Get a configuration value from the server.

        :param connection:
        :param key:
        :return:
        """
        try:
            operation = OOperationRequestConfigList()

            logging.debug("called {}".format(operation))

            response = connection.exec(operation, {})

            return response
        except Exception as err:
            logging.error(err)

    def configget(self, connection:OConnection, key:str):
        """
        Get a configuration value from the server.

        :param connection:
        :param key:
        :return:
        """
        try:
            operation = OOperationRequestConfigGet()

            # data dict
            data = {"key": key}

            logging.debug("called {} with data {}".format(operation, data))

            response = connection.exec(operation, data)

            return response
        except Exception as err:
            logging.error(err)

    def dbcountrecords(self, connection:OConnection):
        """
        Gets the size of the database

        :param connection:
        :return:
        """
        try:
            operation = OOperationDBCountrecords()

            logging.debug("called {}".format(operation))

            response = connection.exec(operation, {})

            return response
        except Exception as err:
            logging.error(err)

    def dbsize(self, connection:OConnection):
        """
        Gets the size of the database

        :param connection:
        :return:
        """
        try:
            operation = OOperationDBSize()

            logging.debug("called {}".format(operation))

            response = connection.exec(operation, {})

            return response
        except Exception as err:
            logging.error(err)



    def recordupdate(self, connection:OConnection, cluster_id:int, cluster_position:int, update_content:bool, record_content:bytes, record_version:int, record_type:ORecordType, mode:OModeInt):
        """
        Tries to update a record

        :param connection:
        :param cluster_id:
        :param cluster_position:
        :param update_content:
        :param record_content:
        :param record_version:
        :param record_type:
        :param mode:
        :return:
        """
        try:
            # prepare data dict
            request_data = {"cluster-id": cluster_id,
                            "cluster-position": cluster_position,
                            "update-content": update_content,
                            "record-content": record_content,
                            "record-version": record_version,
                            "record-type": record_type,
                            "mode": mode}

            operation = OOperationRecordUpdate()

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            return response
        except Exception as err:
            logging.error(err)

    def recorddelete(self, connection:OConnection, cluster_id:int, cluster_position:int, record_version:int, mode:OModeInt):
        """
        Deletes a record, return True if deleted and False if not or not existing record

        :param connection:
        :param cluster_id:
        :param cluster_position:
        :param record_version:
        :param mode:
        :return:
        """
        try:
            # prepare data dict
            request_data = {"cluster-id": cluster_id,
                            "cluster-position": cluster_position,
                            "record-version": record_version,
                            "mode": mode}

            operation = OOperationRecordDelete()

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            if response["payload-status"] != 0:
                return True
            else:
                return False
        except Exception as err:
            logging.error(err)


    def recordload(self, connection:OConnection, cluster_id:int, cluster_position:int, fetch_plan:str, ignore_cache:bytes, load_tombstones:bytes):
        """
        Loads a records from the database

        :param connection:
        :param cluster_id:
        :param cluster_position:
        :param fetch_plan:
        :param ignore_cache:
        :param load_tombstones:
        :return:
        """
        try:
            # prepare data dict
            request_data = {"cluster-id": cluster_id,
                            "cluster-position": cluster_position,
                            "fetch-plan": fetch_plan,
                            "ignore-cache": ignore_cache,
                            "load-tombstones": load_tombstones}

            operation = OOperationRecordLoad()

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            return response
        except Exception as err:
            logging.error(err)

    def recordcreate(self, connection:OConnection, record_content:bytes, record_type:ORecordType, mode:OModeInt):
        """
        Creates a record

        :param connection:
        :param record_content:
        :param record_type:
        :param mode:
        :return:
        """
        try:
            # prepare data dict
            request_data = {"datasegment-id": -1,
                            "cluster-id": -1,
                            "record-content": record_content,
                            "record-type": record_type.value,
                            "mode": mode.value}

            operation = OOperationRecordCreate()

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            return response
        except Exception as err:
            logging.error(err)

    def dbexist(self, connection:OConnection, database_name:str, server_storage_type:OStorageTypes):
        """
        If the database exists it return True otherwise False.
        This operation is only working when the server_connect function has been run preivously.

        :param connection:
        :param database_name:
        :param server_storage_type:
        :return: True if database exists, otherwise False
        """
        try:
            # prepare data dict
            request_data = {"database-name": database_name,
                            "server-storage-type": server_storage_type}

            operation = OOperationDBExist()

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            if response["result"] != 0:
                return True
            else:
                return False
        except Exception as err:
            logging.error(err)

    def command(self, connection:OConnection, mode:OModeChar, class_name:OCommandClass, command_payload):
        """
        Sends a command to the server. There a different modes to tell the server what kind of command has just been sent:
        'q' => idempotent command i.e. SELECT
        'c' => non-idempotent command like INSERT, UPDATE etc.
        's' => script command
        other => a class name which will be invoked on the server using the default constructor

        :param connection:
        :param mode:
        :param class_name:
        :param command_payload_length:
        :param command_payload:
        :return:
        """
        try:
            # prepare data dict
            request_data = {"mode": mode.value,
                            "class-name": class_name.value}


            if isinstance(command_payload, OSQLPayload):
                request_data.update(command_payload.getdata())

            operation = OOperationRequestCommand(command_payload, connection.getprotocolversion())

            if mode == OModeInt.ASYNCHRONOUS:
                operation.setasync(True)

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            return response
        except Exception as err:
            logging.error(err)

    def txcommit(self, connection:OConnection, tx_id:int, using_tx_log:bytes, entries:list):
        """
        Send a bunch of different action to the database to process them in a transaction.

        :param connection:
        :param tx_id:
        :param using_tx_log:
        :param entries:
        :return:
        """
        try:
            entries_profile = list()
            entries_data = list()
            for entry in entries:
                entries_profile.append(entry.getprofile())
                entries_data.append(entry.getdata())

            operation = OOperationRequestTXCommit(entries_profile)

            # prepare data dict
            request_data = {"tx-id": tx_id,
                            "using-tx-log": using_tx_log,
                            "entries": entries_data,
                            "remote-index-length": '',
                            "end": 0}


            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            return response
        except Exception as err:
            # in case of an error terminate the tx
            connection.sendbytes(b'-1')

            logging.error(err)
