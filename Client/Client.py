import requests
import cmd
import socket
import json
import random

# master_ip = input()
# master_port = int(input())
master_ip = "10.192.40.86"
master_port = 5000

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
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if type(address[1]) is not int:
        address[1] = int(address[1])
    try:
        client_socket.connect(tuple(address))
    except ConnectionRefusedError:
        print("Error: Connection refused by Region Server.")
        return
    client_socket.sendall(json_data.encode('utf-8'))
    # 接收服务器返回的数据,超过10秒没有返回数据则超时
    # client_socket.settimeout(10)
    try:
        response = client_socket.recv(1024)
    except socket.timeout:
        print("Error: Timeout when connecting to Region Server.")
        return
    client_socket.close()
    # 如果没有收到数据，就返回
    if not response:
        print("Error: Failed to connect to Region Server: No response.")
        return
    response_data = json.loads(response.decode('utf-8'))
    address = address[0] + ':' + str(address[1])
    if response_data['success'] == 0:
        print(f"Operation failed on {address} : " + response_data['message'])
        return
    elif response_data['success'] == 1:
        print("Create operation succeeded on " + str(address) + " : " + response_data['message'])
    elif response_data['success'] == 2:
        print("Drop operation succeeded on " + str(address) + " : " + response_data['message'])
    elif response_data['success'] == 3:
        print("Write operation succeeded on " + str(address) + " : " + response_data['message'])
    elif response_data['success'] == 4:
        print("Read operation succeeded on " + str(address) + " : " + response_data['message'])
        print("Data: " + str(response_data['data']))

class ClientShell(cmd.Cmd):
    prompt = 'ClientShell> '
    intro = "Welcome to the Python Client Shell. Type help or ? to list commands.\n"

    def do_create(self, line):
        args = line.split(maxsplit=1)
        if len(args) != 2:
            print("Invalid argument. Usage: create <estimated_size> <sql_statement>")
            return
        estimated_size, sql_statement = args
        table_name = sql_statement.split()[2]
        operation = sql_statement.split()[0].upper()
        if operation != 'CREATE':
            print("Invalid operation. Please use CREATE with create.")
            return
        size = int(estimated_size)
        # 如果size小于0，或不是数字
        if size <= 0 or not estimated_size.isdigit():
            print("Invalid estimated size. Please input a positive integer.")
            return
        json_data_to_master = {"table_name": table_name, "estimated_size": size}
        response = send_request_to_master(json_data_to_master, f'http://{master_ip}:{master_port}/api/rounte/create_table')
        if response and response['signal'] == 'success':
            cache[table_name] = response['addresses']
            # 打印cache更新的部分
            print(f"cache updated. add new table to cache: {table_name} -> {response['addresses']}")
            json_data_to_region = {"1": sql_statement}
            for address in response['addresses']:
                send_json_to_region_server(json.dumps(json_data_to_region), list(address.split(':')))
        else:
            print(f"Error: Failed to create table. {response['message']}")

    def help_create(self):
        print('Create a new table. Usage: create <estimated_size> <sql_statement>')

    def do_read(self, line):
        sql_statement = line
        if not sql_statement:
            print("Invalid argument. Usage: read <sql_statement>")
            return
        operation = sql_statement.split()[0].upper()
        if operation != 'SELECT':
            print("Invalid operation. Please use SELECT with read.")
            return
        table_name = sql_statement.split()[3]
        if table_name in cache:
            address = random.choice(cache[table_name])
            print(f"read from {address} from cache")
            send_json_to_region_server(json.dumps({"4": sql_statement}), list(address.split(':')))
        else:
            json_data = {"table_name": table_name}
            response = send_request_to_master(json_data, f'http://{master_ip}:{master_port}/api/rounte/read_table')
            if response and response['signal'] == 'success':
                # cache[table_name] = response['address']
                address = response['address']
                send_json_to_region_server(json.dumps({"4": sql_statement}), list(address.split(':')))
            else:
                print(f"Error: Failed to read from table: {response['message']}")
    
    def help_read(self):
        print('Read from a table. Usage: read <sql_statement>')

    def do_write(self, line):
        sql_statement = line
        # print(sql_statement)
        if not sql_statement:
            print("Invalid argument. Usage: write <sql_statement>")
            return
        operation = sql_statement.split()[0].upper()
        if operation != 'INSERT' and operation != 'UPDATE' and operation != 'DELETE':
            print("Invalid operation. Please use INSERT, UPDATE or DELETE with write.")
            return
        if operation == 'UPDATE':
            table_name = sql_statement.split()[1]
        else:
            table_name = sql_statement.split()[2]
        if table_name in cache:
            for address in cache[table_name]:
                print(f"write to {address} from cache")
                send_json_to_region_server(json.dumps({"3": sql_statement}), list(address.split(':')))
        else:
            json_data = {"table_name": table_name}
            response = send_request_to_master(json_data, f'http://{master_ip}:{master_port}/api/rounte/write_table')
            if response and response['signal'] == 'success':
                cache[table_name] = response['addresses']
                print(f"cache updated. add new table to cache: {table_name} -> {response['addresses']}")
                for address in response['addresses']:
                    send_json_to_region_server(json.dumps({"3": sql_statement}), list(address.split(':')))
            else:
                print(f"Error: Failed to write to table: {response['message']}")

    def help_write(self):
        print('Write to a table. Usage: write <sql_statement>')

    def do_drop(self, line):
        args = line.split()
        if len(args) != 1:
            print("Invalid argument. Usage: drop <table_name>")
            return
        table_name = args[0]
        json_data = {"table_name": table_name}
        response = send_request_to_master(json_data, f'http://{master_ip}:{master_port}/api/rounte/drop_table')
        if response and response['signal'] == 'success':
            if table_name in cache:
                del cache[table_name]
                print(f"cache updated. delete table from cache: {table_name} -> {response['addresses']}")
            sql_statement = "DROP TABLE " + table_name
            for address in response['addresses']:
                send_json_to_region_server(json.dumps({"2": sql_statement}), list(address.split(':')))
        else:
            print(f"Error: Failed to drop table: {response['message']}")

    def help_drop(self):
        print('Drop a table. Usage: drop <table_name>')

    def do_quit(self, line):
        return True

    def help_quit(self):
        print('Quit the shell. You can also use Ctrl-D.')

# main函数入口
if __name__ == "__main__":
    ClientShell().cmdloop()