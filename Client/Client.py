import requests

def get_region_server_ip(zookeeper_host, table_name):
    url = f"http://{zookeeper_host}/get_region_server_ip"
    # 构造请求数据
    params = {
        "table_name": table_name
    }

    try:
        response = requests.get(url, params=params)

        # 成功请求
        if response.status_code == 200:
            region_server_info = response.json()
            return region_server_info['ip'], region_server_info['port'], region_server_info['path']
        else:
            print("Error: Failed to get region server information from Zookeeper.")
            return None, None, None
    except Exception as e:
        print(f"Error: {e}")
        return None, None, None


def get_rpc_file(region_server_ip, region_server_port, file_path):
    url = f"http://{region_server_ip}:{region_server_port}/{file_path}"

    try:
        # 发送请求到region_guard获取RPC文件，此处先模拟为一个普通文件
        response = requests.get(url)

        # 文件保存本地
        if response.status_code == 200:
            with open("rpc_file.txt", "wb") as f:
                f.write(response.content)
            print("File saved successfully.")
        else:
            print("Error: Failed to get file from RegionServer.")
    except Exception as e:
        print(f"Error: {e}")

# main函数入口
if __name__ == "__main__":
    zoo_keeper_host = '127.0.0.1:2181'
    table_name = input("table name: ")

    region_server_ip, region_server_port, file_path = get_region_server_ip(zoo_keeper_host, table_name)

    if region_server_ip and region_server_port:
        # 获取并保存本地
        get_rpc_file(region_server_ip, region_server_port, file_path)
