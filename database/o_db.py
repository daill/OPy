from database.o_db_connection import OConnection
from database.o_db_constants import ODBType
from database.protocol.o_ops import OClient

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
    o_db_client = OClient()

    connection = OConnection("root", "root")
    # data = db_exist(connection, "GratefulDeadConcerts", OStorageTypes.PLOCAL)
    # print(data)
    print(o_db_client.connect(connection, user_name="root", user_password="root"))
    print(o_db_client.db_open(connection, database_name="GratefulDeadConcerts", database_type=ODBType.DOCUMENT, user_name="root", user_password="root"))
    print(o_db_client.db_size(connection))
    print(o_db_client.db_countrecords(connection))
    content = 'Profile@nick:"ThePresident",follows:[],followers:[#10:5,#10:6],name:"Barack",surname:"Obama",location:#3:2,invitedBy:,salary_cloned:,salary:120.3f'
    print(o_db_client.record_load(connection, cluster_id=9, cluster_position=2, fetch_plan="", ignore_cache=1, load_tombstones=' '))
    o_db_client.db_close(connection)



def string_bytes(string):
  print(len(string))


if __name__ == "__main__":
  do()