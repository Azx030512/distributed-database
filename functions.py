import socket
from kazoo.client import KazooClient
from kazoo.protocol.states import EventType
from flask import Flask, jsonify
from flask import make_response
from flask import request
from flask import abort
from pprint import pprint 
import time
import json
import os

zoo_keeper_host = '127.0.0.1:2181'
master_node_path = "/master"
region_node_path = '/region'

def ip_address():
    st = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:       
        st.connect(('10.255.255.255', 1))
        IP = st.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        st.close()
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
    return child_data

def debug_zookeeper(zookeeper):
    nodes = zookeeper.get_children('/') 
    print('nodes',nodes)
    for node in nodes:
        node_data, _ = zookeeper.get(node)
        print("node data:", node_data.decode("utf-8"))
        get_node_status(zookeeper, node)

def prepare_node_data(json_path="./database_information.json"):
    with open(json_path, "r") as fp:
        database_information = json.load(fp)
    tables = []
    for region in database_information.keys():
        tables += database_information[region]['tables']
    tables = list(set(tables))
    node_data = {
        "ip-address": ip_address(),
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
    decrease_index = sorted(range(len(free_spaces)), key=lambda k: free_spaces[k], reverse=True)
    select_index = decrease_index[:top_k]
    select_regions = []
    for index in select_index:
        select_regions.append(online_regions[index])
    return select_regions

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
        node_data['database_information'][region]['tables'].append((table_name,estimated_size))
    if len(allocated_regions)>0:
        node_data['tables'].add((table_name,estimated_size))
    return allocated_regions, node_data

def regions_address(zookeeper, regions: list):
    addresses = []
    for region in regions:
        address = get_node_address(zookeeper, os.path.join(region_node_path, region))
        addresses.append(address)
    return addresses