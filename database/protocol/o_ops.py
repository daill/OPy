import logging

from database.o_db_connection import OConnection
from database.o_db_constants import OStorageTypes, ODBType, ODriver, OModeInt, ORecordType, OModeChar
from database.protocol.o_op_connect import OOperationConnect
from database.protocol.o_op_db import OOperationDBClose, OOperationDBCreate, OOperationDBExist, OOperationDBList, \
    OOperationDBOpen, OOperationDBReload, \
    OOperationDBSize, OOperationDBCountrecords
from database.protocol.o_op_record import OOperationRecordCreate, OOperationRecordLoad, OOperationRecordUpdate, \
    OOperationRecordDelete
from database.protocol.o_op_request import OOperationRequestConfigGet, OOperationRequestConfigList, \
    OOperationRequestConfigSet, OOperationRequestCommand, OSQLPayload


__author__ = 'daill'


class OClient:
    def db_reload(self, connection:OConnection):
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

    def db_create(self, connection:OConnection, database_name:str, database_type:ODBType, storage_type:OStorageTypes):
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
                    "protocol-version": connection.get_protocol_version(),
                    "client-id": '-1',
                    "user-name": user_name,
                    "user-password": user_password}

            result_data = connection.exec(operation_connect, data)

            return result_data
        except Exception as err:
            logging.error(err)

    def db_open(self, connection:OConnection, database_name:str, database_type:ODBType, user_name:str,
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
                            "protocol-version": connection.get_protocol_version(),
                            "client-id": "",
                            "user-name": user_name,
                            "user-password": user_password,
                            "database-name": database_name,
                            "database-type": database_type}

            operation = OOperationDBOpen()

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            return response
        except Exception as err:
            logging.error(err)

    def db_close(self, connection:OConnection):
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

    def config_set(self, connection:OConnection, key:str, value:str):
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

    def db_list(self, connection:OConnection):
        """
        Gets the size of the database

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

    def config_list(self, connection:OConnection):
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

    def config_get(self, connection:OConnection, key:str):
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

    def db_countrecords(self, connection:OConnection):
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

    def db_size(self, connection:OConnection):
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

    def db_list(self, connection:OConnection):
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


    def record_update(self, connection:OConnection, cluster_id:int, cluster_position:int, update_content:bool, record_content:bytes, record_version:int, record_type:ORecordType, mode:OModeInt):
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

    def record_delete(self, connection:OConnection, cluster_id:int, cluster_position:int, record_version:int, mode:OModeInt):
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


    def record_load(self, connection:OConnection, cluster_id:int, cluster_position:int, fetch_plan:str, ignore_cache:bytes, load_tombstones:bytes):
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

    def record_create(self, connection:OConnection, record_content:bytes, record_type:ORecordType, mode:OModeInt):
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
                            "record-type": record_type,
                            "mode": mode}

            operation = OOperationRecordCreate()

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            return response
        except Exception as err:
            logging.error(err)

    def db_exist(self, connection:OConnection, database_name:str, server_storage_type:OStorageTypes):
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

    def command(self, connection:OConnection, mode:OModeChar, class_name:str, command_payload):
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
                            "class-name": class_name}


            if isinstance(command_payload, OSQLPayload):
                request_data.update(command_payload.get_data())

            operation = OOperationRequestCommand(command_payload, connection.get_protocol_version())

            if mode == OModeInt.ASYNCHRONOUS:
                operation.set_async(True)

            logging.debug("called {} with data {}".format(operation, request_data))

            response = connection.exec(operation, request_data)

            return response
        except Exception as err:
            logging.error(err)