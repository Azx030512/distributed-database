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

    except Exception as e:
        return jsonify({'message': "消息列表查询失败！",
                        'signal': "fail"}), 500


if __name__ == "__main__":
    try:
        api.run(debug=False, port=master_port, host='127.0.0.1')
    except:
        input("Type Enter exit.")
    zk.stop()
