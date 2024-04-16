import socket
from kazoo.client import KazooClient
from kazoo.protocol.states import EventType
from flask import Flask, jsonify
from flask import make_response
from flask import request
from flask import abort
from pprint import pprint 
import random
import time
import json
import os

zoo_keeper_host = '127.0.0.1:2181'
master_node_path = "/master"
region_node_path = '/region'

def ip_address(port=None):
    st = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:       
        st.connect(('10.255.255.255', 1))
        IP = st.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        st.close()
    if port:
        IP = IP + ':' + str(port)
    return IP

def get_node_status(zookeeper, node_path):
    online = False
    if zookeeper.exists(node_path):
        child_data, child_stat = zookeeper.get(node_path)
        if child_stat.ephemeralOwner != 0:
            online = True
        else:
            online = False
    else:
        online = False
    return online

def get_node_address(zookeeper, node_path):
    child_data=None
    if zookeeper.exists(node_path):
        child_data, child_stat = zookeeper.get(node_path)
    return child_data.decode('utf-8')

def debug_zookeeper(zookeeper):
    node_data, _ = zookeeper.get('/master')
    print('/master')
    pprint(json.loads(node_data.decode("utf-8")))
    nodes = zookeeper.get_children('/region') 
    pprint(nodes)
    for node in nodes:
        node_data, _ = zookeeper.get(os.path.join('/region', node))
        print(node)
        pprint(node_data.decode("utf-8"))

def prepare_node_data(port, json_path="./database_information.json"):
    with open(json_path, "r") as fp:
        database_information = json.load(fp)
    tables = []
    for region in database_information.keys():
        for table in  database_information[region]['tables']:
            tables.append(table[0])
    tables = list(set(tables))
    node_data = {
        "ip-address": ip_address(port = port),
        "database_information": database_information,
        "tables": tables
    }
    return node_data

def solidate_node_data(zookeeper, node_data, json_path="./database_information.json"):
    with open(json_path, "w") as fp:
        json.dump(node_data['database_information'], fp)
    zookeeper.set(master_node_path, json.dumps(node_data).encode("utf-8"))
    return node_data

def storage_balance(online_regions, free_spaces, top_k = 2):
    # use greedy algorithm to balance storage load
    decrease_index = sorted(range(len(free_spaces)), key=lambda k: free_spaces[k], reverse=True)
    select_index = decrease_index[:top_k]
    select_regions = []
    for index in select_index:
        select_regions.append(online_regions[index])
    return select_regions

def load_balance(addresses):
    random.seed(time.time())
    address = random.choice(addresses)
    return address

def allocate_region(zookeeper, node_data, table_name, estimated_size):
    online_regions = []
    free_spaces = []
    for region in node_data['database_information'].keys():
        if node_data['database_information'][region]['online']:
            online_regions += [region]
            free_spaces += [node_data['database_information'][region]['free_space']]
    allocated_regions = storage_balance(online_regions, free_spaces)
    for region in allocated_regions:
        node_data['database_information'][region]['free_space'] -= estimated_size
        node_data['database_information'][region]['tables'].append(tuple([table_name,estimated_size]))
    if len(allocated_regions)>0:
        node_data['tables'].append((table_name,estimated_size))
    return allocated_regions, node_data

def regions_address(zookeeper, regions: list):
    addresses = []
    for region in regions:
        address = get_node_address(zookeeper, os.path.join(region_node_path, region))
        addresses.append(address)
    return addresses

def table_exists(node_data, table_name):
    if table_name in node_data['tables']:
        return True
    else:
        return False

def search_tables(node_data, table_name):
    target_regions = []
    online_statuses = []
    for region in node_data['database_information'].keys():
        table_names = [table[0] for table in node_data['database_information'][region]['tables']]
        if table_name in table_names:
            target_regions += [region]
            online_statuses += [node_data['database_information'][region]['online']]
    return target_regions, online_statuses

def online_region_filter(regions, online_statuses):
    online_regions = []
    for i in range(len(regions)):
        if online_statuses[i]:
            online_regions.append(regions[i])
    return online_regions, len(online_regions)

def unregister_table(node_data, online_regions, table_name):
    for region in online_regions:
        for i in range(len(node_data['database_information'][region])):
            if table_name == node_data['database_information'][region]['tables'][i][0]:
                node_data['database_information'][region]['tables'].__delitem__(i)
                node_data['database_information'][region]['free_space'] += node_data['database_information'][region]['tables'][i][1]
                break
    return node_data