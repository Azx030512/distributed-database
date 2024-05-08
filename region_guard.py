from functions import *
from region_functions import *
import threading

region_name = input('region name:')
region_port = int(input('region port:'))

host = "localhost"
user = "root"
password = "jin751120zzw"
database = "region1"

#client_host = ""
client_port = ""




if __name__ == "__main__":
    zk = KazooClient(hosts=zoo_keeper_host)
    zk.start() 
    #create ephemeral master node to indicate node state online or not 
    region_node_path = os.path.join(region_node_path, region_name)
    print("region_node_path:", region_node_path)
    zk.create(region_node_path, ip_address(port = region_port).encode("utf-8"), ephemeral=True, makepath=True)
    input("wait...")
    zk.stop()


#向客户端发送对数据库的操作结果
def send_message(host, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(message.encode())

# 接受对于数据库进行操作的消息， 与客户端通信时
# 1 -> 建表
# 2 -> 删表
# 3 -> 改表 
# 4 -> 查表


# 1 -> 操作成功
# 0 -> 操作失败

def receive_client_massage(received_json_data, client_host):
    parsed_data = json.loads(received_json_data)
    print("接收到的JSON数据:", parsed_data)
    #遍历JSON数据，根据键的值进行分类处理
    for key, value in parsed_data.items():
        if key == "1":
        # 进行建表操作
            response = create_region_table(host, user, password, database, value)
            send_message(client_host, client_port, response)

        elif key == "2":
        # 进行删表操作
            response = drop_region_table(host, user, password, database, value)
            send_message(client_host, client_port, response)

        elif key == "3":
        # 进行改表操作
            response = alter_region_table(host, user, password, database, value)
            send_message(client_host, client_port, response)

        elif key == "4":
        # 进行查表操作
            response = query_region_table(host, user, password, database, value)
            send_message(client_host, client_port, response)

        else:
        # 未知类型操作
            print(f"Unknown type - Key: {key}, Value: {value}")


def zxhlll(connection, client_address):
    # 接收数据
    data = b""
    while True:
        chunk = connection.recv(1024)
        if not chunk:
            break
        data += chunk

    # 解析JSON数据
    received_json_data = data.decode("utf-8")
    receive_client_massage(received_json_data, client_address)



# 监听socket通讯

# 创建一个TCP/IP套接字
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# 绑定套接字到端口
server_address = ('127.0.0.1', 12345)  # 这里的端口可以根据需要修改
server_socket.bind(server_address)

# 开始监听传入的连接
server_socket.listen(1)

while True:
    print("等待连接...")
    connection, client_address = server_socket.accept()

    try:
        print("连接已建立：", client_address)

        # 创建线程
        thread1 = threading.Thread(target=zxhlll, args=(connection, client_address))
        # 启动线程
        thread1.start()
        # 等待线程执行完成
        thread1.join()


    finally:
        # 关闭连接
        connection.close()
