import sys
import subprocess

def create_all_brokers():
    command = r"wt -d C:\Users\Bhavini\Git Repos\BD2_005_081_082_088\broker ; --title broker; "
    to_port_no = 3000
    for i in range(1, 4):
        id = '00' + str(i)
        port_no = str(5000 + i)
        if i == 1:
            command += r" sp python broker.py " + \
                str(id) + " " + str(port_no) + " " + str(to_port_no) + " 1 ;"
        else:
            command += r" sp python broker.py " + \
                str(id) + " " + str(port_no) + " " + str(to_port_no) + " 0 ;"
    command = command[:-1]
    subprocess.run(command, shell=True)

def create_one_broker(bid):
    command = "wt --title broker; "
    to_port_no = 3000
    id = '00' + str(bid)
    port_no = str(5000 + bid)
    command += r" sp python broker.py " + \
        str(id) + " " + str(port_no) + " " + str(to_port_no) + " 0 ;"
    command = command[:-1]
    subprocess.run(command, shell=True)
