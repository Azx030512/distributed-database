import socket
import json

# 创建一个TCP/IP套接字
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# 连接服务器
server_address = ('127.0.0.1', 12345)  # 这里的端口和服务器端对应
client_socket.connect(server_address)

try:
    # 准备要发送的JSON数据

    # 获取用户输入
    user_input = input("请输入数据: ")

    # 创建一个字典，将用户输入作为值
    data = {"4": user_input}

    # 将字典转换为JSON格式的字符串
    json_data = json.dumps(data)

    # 打印JSON格式的数据
    print("转换为JSON格式的数据:", json_data)

    # 发送数据
    client_socket.sendall(json_data.encode("utf-8"))

finally:
    # 关闭连接
    client_socket.close()