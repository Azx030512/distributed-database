from functions import *
from region_functions import *
import threading

region_name = input('region name:')
region_port = int(input('region port:'))

host = "localhost"
user = "root"
password = "azx"
database = "region1"




#向客户端发送对数据库的操作结果
def send_message(connection, message):
    try:
        result = connection.sendall(message.encode())
        print(result)
    except Exception as e:
        return

# 接受对于数据库进行操作的消息， 与客户端通信时
# 1 -> 建表
# 2 -> 删表
# 3 -> 改表 
# 4 -> 查表

#回复的json格式
# success：true -> 操作成功/false -> 操作失败   | message：具体的回复消息 |data（对表查询结果才会有）

def receive_client_massage(received_json_data, connection):
    parsed_data = json.loads(received_json_data)
    print("接收到的JSON数据:", parsed_data)
    #遍历JSON数据，根据键的值进行分类处理
    for key, value in parsed_data.items():
        if key == "1":
        # 进行建表操作
            response = create_region_table(host, user, password, database, value)
            send_message(connection, response)

        elif key == "2":
        # 进行删表操作
            response = drop_region_table(host, user, password, database, value)
            send_message(connection, response)

        elif key == "3":
        # 进行改表操作
            response = alter_region_table(host, user, password, database, value)
            send_message(connection, response)

        elif key == "4":
        # 进行查表操作
            response = query_region_table(host, user, password, database, value)
            send_message(connection, response)

        else:
        # 未知类型操作
            print(f"Unknown type - Key: {key}, Value: {value}")


def process(connection):
    # 接收数据
    data = b""
    data = connection.recv(4096)
    # 解析JSON数据
    received_json_data = data.decode("utf-8")
    receive_client_massage(received_json_data, connection)
    connection.close()


if __name__ == "__main__":
    zk = KazooClient(hosts=zoo_keeper_host)
    zk.start() 
    #create ephemeral master node to indicate node state online or not 
    region_node_path = os.path.join(region_node_path, region_name)
    print("region_node_path:", region_node_path)
    zk.create(region_node_path, ip_address(port = region_port).encode("utf-8"), ephemeral=True, makepath=True)

    # 监听socket通讯
    # 创建一个TCP/IP套接字
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 绑定套接字到端口
    server_address=ip_address()
    print('server address:', server_address)
    server_socket.bind((server_address,region_port))

    #开始监听传入的连接
    server_socket.listen(5)
    
    while True:
        try:
            print("等待连接...")
            connection, client_address = server_socket.accept() 
            print("连接已建立：", client_address)
            # 创建线程
            process(connection)

        except KeyboardInterrupt:
            zk.stop()
            print("服务器关闭")
            break

        except Exception as e:
            # 重新创建 socket 对象
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address=ip_address()
            print('server address:', server_address)
            server_socket.bind((server_address,region_port))
            server_socket.listen(5)
        






