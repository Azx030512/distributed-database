from functions import *
region_name = "azx"


# import pymysql


import mysql.connector


if __name__ == "__main__":
    zk = KazooClient(hosts=zoo_keeper_host)
    zk.start() 
    #create ephemeral master node to indicate node state online or not 
    zk.create(os.path.join(region_node_path, region_name), ip_address().encode("utf-8"), ephemeral=True, makepath=True)
    input("wait...")
    zk.stop()

# 连接到数据库
connection = mysql.connector.connect(
  host="localhost",
  user="root",
  password="jin751120zzw",
  database="region1"
)

if connection.is_connected():
    print("Connected to MySQL database")

# 接受对于数据库进行操作的消息




# 在这里执行数据库操作
def delete():
    
def update():

    
def quary():



# 关闭连接
connection.close()
