import os
system = input('choose system windows or ubuntu:')
if system == 'windows':
    os.system('./apache-zookeeper-3.8.4-bin/bin/zkServer.cmd start')
else:
    os.system('./apache-zookeeper-3.8.4-bin/bin/zkServer.sh start')