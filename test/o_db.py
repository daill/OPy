from database.o_db_connection import OConnection
from database.o_db_constants import ODBType
from database.o_db_ops import ODB

__author__ = 'daill'


def do():
    # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # s.connect(("0.0.0.0", 2424))
    # reply = s.recv(4096)
    #
    # print(struct.unpack(">h", reply)[0])
    #
    # message = struct.pack("> b i i 3s i s h i i 23s i 4s i 4s", 2, -1, 3, b"pyo", len("a"), b"a", 21, (-1),  len("ORecordSerializerBinary"), b"ORecordSerializerBinary", len("root"), b"root", len("root"), b"root")
    # # message = struct.pack("> b i 1s i s h i i 23s i 4s i 4s", 2, 1, str.encode("P"), len("a"), b"a", 21, (-1),  len("ORecordSerializerBinary"), b"ORecordSerializerBinary", len("root"), b"root", len("root"), b"root")
    #
    # print(message)
    # result = s.sendall(message)
    #
    #
    #
    # print(result)
    # while True:
    #   data = s.recv(4096)
    #   a = data[:1]
    #   if a:
    #     print(struct.unpack('>b', a)[0])
    #   if not data: break
    # conn = Connection("0.0.0.0", 2424)
    # print(conn.protocol_version)
    odb = ODB()

    connection = OConnection()
    # data = db_exist(connection, "GratefulDeadConcerts", OStorageTypes.PLOCAL)
    # print(data)
    print(odb.connect(connection, user_name="root", user_password="root"))
    print(odb.dbopen(connection, database_name="Crwlr", database_type=ODBType.DOCUMENT.value, user_name="root", user_password="root"))
    # print(odb.db_size(connection))
    # print(odb.db_countrecords(connection))
    content1 = 'Profile@nick:"ThePresident",follows:[],followers:[#10:5,#10:6],name:"Barack",surname:"Obama",location:#3:2,invitedBy:,salary_cloned:,salary:120.3f'
    content2 = 'Profile@nick:"ThePresident",follows:[],followers:[#10:5,#10:6],name:"Angela",surname:"Merkel",location:#3:2,invitedBy:,salary_cloned:,salary:192.0f'
    # print(odb.record_create(connection, mode=OModeInt.SYNCHRONOUS.value, record_content=content, record_type=ORecordType.DOCUMENT.value))
    print(odb.recordload(connection, cluster_id=12, cluster_position=1, fetch_plan="", ignore_cache=1, load_tombstones=' '))
    # print(odb.record_delete(connection, cluster_id=13, cluster_position=11, record_version=2, mode=OModeInt.SYNCHRONOUS.value))
    # command = OSQLCommand("create vertex Person set name = 'Luca1'", non_text_limit=-1, fetchplan="*:0", serialized_params="")
    # print(odb.command(connection, mode=OModeChar.SYNCHRONOUS, class_name=OCommandClass.NON_IDEMPOTENT, command_payload=command))
    # create1 = OTXOperationCreate(record_type=ORecordType.DOCUMENT.value, record_content=content1, record_id=-2)
    # create2 = OTXOperationCreate(record_type=ORecordType.DOCUMENT.value, record_content=content2, record_id=-3)
    # delete = OTXOperationDelete(record_type=ORecordType.DOCUMENT.value, cluster_id=13, cluster_position=14, version=2)
    # print(odb.tx_commit(connection, random.randint(1,1000), 0, [create1, create2]))

    odb.dbclose(connection)

if __name__ == "__main__":
  do()

