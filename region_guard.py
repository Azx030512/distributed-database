from functions import *
region_name = input('region name:')
region_port = int(input('region port:'))

if __name__ == "__main__":
    zk = KazooClient(hosts=zoo_keeper_host)
    zk.start() 
    #create ephemeral master node to indicate node state online or not 
    region_node_path = os.path.join(region_node_path, region_name)
    print("region_node_path:", region_node_path)
    zk.create(region_node_path, ip_address(port = region_port).encode("utf-8"), ephemeral=True, makepath=True)
    input("wait...")
    zk.stop()

