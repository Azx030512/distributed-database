import socket
from kazoo.client import KazooClient
from kazoo.protocol.states import EventType
from flask import Flask, jsonify
from flask import make_response
from flask import request
from flask import abort
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

def debug_zookeeper(zookeeper):
    nodes = zookeeper.get_children('/') 
    print('nodes',nodes)
    for node in nodes:
        node_data, _ = zookeeper.get(node)
        print("node data:", node_data.decode("utf-8"))
        get_node_status(zookeeper, node)