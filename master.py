from functions import *

with open("./database_information.json","r+") as fp:
    database_information = json.load(fp)

node_data = {
    "ip-address": ip_address(),
    "database_information": database_information
}

zk = KazooClient(hosts=zoo_keeper_host)
zk.start() 
#create ephemeral master node to indicate node state online or not 
if not zk.exists(master_node_path):
    zk.create(master_node_path, json.dumps(node_data).encode("utf-8"), ephemeral=False, makepath=True)

@zk.ChildrenWatch(region_node_path)
def watchRegionServer(children):
    print('children', children)
    region_server_list = list(node_data["database_information"].keys())+children
    if region_server_list is not None:
        region_server_set=set(region_server_list)
    else:
        region_server_set=set()
    for region_server in region_server_set:
        if region_server not in node_data["database_information"]:
            node_data["database_information"][region_server]={}
            node_data["database_information"][region_server]['database'] = []
        if region_server in children:
            node_data["database_information"][region_server]['online']=True
        else:
            node_data["database_information"][region_server]['online']=False
    zk.set(master_node_path, json.dumps(node_data).encode("utf-8"))
    with open("./database_information.json","r+") as fp:
        json.dump(database_information, fp)
    debug_zookeeper(zk)

if __name__ == "__main__":
    try:
        while True:
            time.sleep(1)
    except:
        input("Type Enter exit.")
    zk.stop()
