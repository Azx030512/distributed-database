from functions import *
master_port = 5000
node_data = prepare_node_data(master_port)

zk = KazooClient(hosts=zoo_keeper_host)
zk.start()
# create ephemeral master node to indicate node state online or not
if not zk.exists(master_node_path):
    zk.create(master_node_path, json.dumps(node_data).encode("utf-8"), ephemeral=False, makepath=True)


@zk.ChildrenWatch(region_node_path)
def watchRegionServer(children):
    print('children', children)
    global node_data
    region_server_list = list(node_data["database_information"].keys())+children
    if region_server_list is not None:
        region_server_set = list(set(region_server_list))
    else:
        region_server_set = []
    for region_server in region_server_set:
        if region_server not in node_data["database_information"]:
            node_data["database_information"][region_server] = {}
            node_data["database_information"][region_server]['tables'] = []
            node_data["database_information"][region_server]['free_space'] = 1024
        if region_server in children:
            node_data["database_information"][region_server]['online'] = True
        else:
            node_data["database_information"][region_server]['online'] = False
    node_data = solidate_node_data(zk, node_data)
    debug_zookeeper(zk)


api = Flask('master_api')

@api.route('/api/rounte/create_table', methods=['POST'])
def create_table():
    try:
        requirement_json = request.json
        pprint(requirement_json)
        table_name = requirement_json['table_name']
        estimated_size = requirement_json['estimated_size']
        global zk
        global node_data
        if not table_exists(node_data, table_name):
            allocated_regions, node_data = allocate_region(zk, node_data, table_name, estimated_size)
            solidate_node_data(zk, node_data)
            addresses = regions_address(zk, allocated_regions)
            return jsonify(
                {
                    'message': "Table creation success!",
                    'signal': "success",
                    'addresses': addresses
                }
                ), 200
        else:
            return jsonify(
                {
                    'message': "Table already exsits!",
                    'signal': "fail",
                    'addresses': []
                }
                ), 200

    except Exception as e:
        return jsonify({'message': "Error!",
                        'signal': "fail"}), 500

@api.route('/api/rounte/read_table', methods=['POST'])
def read_table():
    try:
        requirement_json = request.json
        pprint(requirement_json)
        table_name = requirement_json['table_name']
        global zk
        global node_data
        if table_exists(node_data, table_name):
            target_regions, online_statuses = search_tables(node_data, table_name)
            online_regions, online_number = online_region_filter(target_regions, online_statuses)
            if online_number>0:
                addresses = regions_address(zk, online_regions)
                address = load_balance(addresses)
                return jsonify(
                    {
                        'message': "Table search success!",
                        'signal': "success",
                        'address': address
                    }
                    ), 200
            else:
                return jsonify(
                    {
                        'message': "No available server!",
                        'signal': "fail",
                        'address': ''
                    }
                    ), 200
        else:
            return jsonify(
                {
                    'message': "Table not exsits!",
                    'signal': "fail",
                    'address': ''
                }
                ), 200

    except Exception as e:
        return jsonify({'message': "Error!",
                        'signal': "fail"}), 500

@api.route('/api/rounte/write_table', methods=['POST'])
def write_table():
    try:
        requirement_json = request.json
        pprint(requirement_json)
        table_name = requirement_json['table_name']
        global zk
        global node_data
        if table_exists(node_data, table_name):
            target_regions, online_statuses = search_tables(node_data, table_name)
            online_regions, online_number = online_region_filter(target_regions, online_statuses)
            if online_number == len(target_regions):
                addresses = regions_address(zk, online_regions)
                return jsonify(
                    {
                        'message': "Table search success!",
                        'signal': "success",
                        'addresses': addresses
                    }
                    ), 200
            else:
                return jsonify(
                    {
                        'message': "Not all servers are available!",
                        'signal': "fail",
                        'addresses': []
                    }
                    ), 200
        else:
            return jsonify(
                {
                    'message': "Table not exsits!",
                    'signal': "fail",
                    'addresses': []
                }
                ), 200

    except Exception as e:
        return jsonify({'message': "Error!",
                        'signal': "fail"}), 500

@api.route('/api/rounte/drop_table', methods=['POST'])
def drop_table():
    try:
        requirement_json = request.json
        pprint(requirement_json)
        table_name = requirement_json['table_name']
        global zk
        global node_data
        if table_exists(node_data, table_name):
            target_regions, online_statuses = search_tables(node_data, table_name)
            online_regions, online_number = online_region_filter(target_regions, online_statuses)
            if online_number == len(target_regions):
                addresses = regions_address(zk, online_regions)
                node_data = unregister_table(node_data, online_regions, table_name)
                solidate_node_data(zk, node_data)
                node_data = prepare_node_data(master_port)
                return jsonify(
                    {
                        'message': "Table drop success!",
                        'signal': "success",
                        'addresses': addresses
                    }
                    ), 200
            else:
                return jsonify(
                    {
                        'message': "Not all servers are available!",
                        'signal': "fail",
                        'addresses': []
                    }
                    ), 200
        else:
            return jsonify(
                {
                    'message': "Table not exsits!",
                    'signal': "fail",
                    'addresses': []
                }
                ), 200

    except Exception as e:
        return jsonify({'message': "Error!",
                        'signal': "fail"}), 500

if __name__ == "__main__":
    try:
        api.run(debug=False, port=master_port, host='127.0.0.1')
    except:
        input("Type Enter exit.")
    zk.stop()
