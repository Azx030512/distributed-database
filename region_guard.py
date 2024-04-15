from functions import *
region_name = "azx"

if __name__ == "__main__":
    zk = KazooClient(hosts=zoo_keeper_host)
    zk.start() 
    #create ephemeral master node to indicate node state online or not 
    zk.create(os.path.join(region_node_path, region_name), ip_address().encode("utf-8"), ephemeral=True, makepath=True)
    input("wait...")
    zk.stop()

