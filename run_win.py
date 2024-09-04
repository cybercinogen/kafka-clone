import sys
import subprocess
from broker.broker_mom import create_all_brokers

# the CLAs are number of producers, number of consumers
nop = int(sys.argv[1])
noc = int(sys.argv[2])

# start with zookeeper
zookeeper_port = str(3000)
# command = r"wt --title zookeeper ; python zookeeper\zookeeper.py 001 3000"
# subprocess.run(command, shell=True)

# start brokers
# create_all_brokers()

# create consumers
command = "wt --title consumer;"
for i in range(1, noc+1):
    id = '00' + str(i)
    port_no = str(3050 + i)
    topic = str(input("enter the topic of consumer " + id))
    command += r" sp python consumers\consumer.py " + \
        str(id) + " " + str(port_no) + " " + zookeeper_port + " " + topic + ";"
command = command[:-1]
subprocess.run(command, shell=True)

# create producers
command = "wt --title producer ; "
for i in range(1, nop+1):
    id = str(i)
    port_no = str(3150 + i)
    command += r" sp python producers\producer.py " + \
        str(id) + " " + str(port_no) + " " + zookeeper_port + ";"
command = command[:-1]
subprocess.run(command, shell=True)
