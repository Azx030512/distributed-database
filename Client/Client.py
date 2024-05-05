import requests
import cmd
import socket
import json

# 定义一个简单的内存缓存
cache = {}

def send_request_to_master(json_data, url):
    response = requests.post(url, json=json_data)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error: Failed to connect to Master.")
        return None

def send_json_to_region_server(json_data, address):
    # 创建一个TCP/IP套接字
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # 连接到服务器
    client_socket.connect(address)
    # 发送数据
    client_socket.sendall(json_data.encode('utf-8'))
    # 关闭套接字
    client_socket.close()

class ClientShell(cmd.Cmd):
    prompt = 'ClientShell> '
    intro = "Welcome to the Python Client Shell. Type help or ? to list commands.\n"

    def do_create(self, line):
        args = line.split()
        if len(args) != 2:
            print("Invalid argument. Usage: create <table_name> <estimated_size>")
            return
        table_name = args[0]
        estimated_size = args[1]
        json_data = {"table_name": table_name, "estimated_size": estimated_size}
        response = send_request_to_master(json_data, 'http://127.0.0.1:5000/api/rounte/create_table')
        if response and response['signal'] == 'success':
            cache[table_name] = response['addresses']
            for address in response['addresses']:
                send_json_to_region_server(json.dumps({"1": table_name}), tuple(address.split(':')))

    def help_create(self):
        print('Create a new table. Usage: create <table_name> <estimated_size>')

    def do_read(self, line):
        args = line.split()
        if len(args) != 1:
            print("Invalid argument. Usage: read <table_name>")
            return
        table_name = args[0]
        if table_name in cache:
            send_json_to_region_server(json.dumps({"4": table_name}), tuple(cache[table_name][0].split(':')))
        else:
            json_data = {"table_name": table_name}
            response = send_request_to_master(json_data, 'http://127.0.0.1:5000/api/rounte/read_table')
            if response and response['signal'] == 'success':
                cache[table_name] = [response['address']]
                send_json_to_region_server(json.dumps({"4": table_name}), tuple(response['address'].split(':')))

    def help_read(self):
        print('Read a table. Usage: read <table_name>')

    def do_write(self, line):
        args = line.split()
        if len(args) != 2:
            print("Invalid argument. Usage: write <table_name> <new_data>")
            return
        table_name = args[0]
        new_data = args[1]
        if table_name in cache:
            for address in cache[table_name]:
                send_json_to_region_server(json.dumps({"3": {table_name: new_data}}), tuple(address.split(':')))
        else:
            json_data = {"table_name": table_name}
            response = send_request_to_master(json_data, 'http://127.0.0.1:5000/api/rounte/write_table')
            if response and response['signal'] == 'success':
                cache[table_name] = response['addresses']
                for address in response['addresses']:
                    send_json_to_region_server(json.dumps({"3": {table_name: new_data}}), tuple(address.split(':')))

    def help_write(self):
        print('Write to a table. Usage: write <table_name> <new_data>')

    def do_drop(self, line):
        args = line.split()
        if len(args) != 1:
            print("Invalid argument. Usage: drop <table_name>")
            return
        table_name = args[0]
        json_data = {"table_name": table_name}
        response = send_request_to_master(json_data, 'http://127.0.0.1:5000/api/rounte/drop_table')
        if response and response['signal'] == 'success':
            if table_name in cache:
                del cache[table_name]
            for address in response['addresses']:
                send_json_to_region_server(json.dumps({"2": table_name}), tuple(address.split(':')))

    def help_drop(self):
        print('Drop a table. Usage: drop <table_name>')

    def do_quit(self, line):
        return True

    def help_quit(self):
        print('Quit the shell. You can also use Ctrl-D.')

# main函数入口
if __name__ == "__main__":
    ClientShell().cmdloop()