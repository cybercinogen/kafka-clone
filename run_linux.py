import sys
import subprocess
# the CLAs are number of producers, number of consumers
nop = int(sys.argv[1])
noc = int(sys.argv[2])
for i in range(1, nop+1):
    id = str(i)
    port_no = str(3000+i)
    command = "gnome-terminal -x \"python3 ./producers/producer.py" + \
        str(id)+" "+str(port_no)+"\""
    subprocess.run(command, shell=True)
for i in range(1, noc+1):
    id = str(i)
    port_no = str(3000+i)
    to_port_no = str(4000+i)
    command = "gnome-terminal -x \"python3 ./consumers/consumer.py " + \
        str(id)+" "+str(port_no)+" "+str(to_port_no)+"\""
    subprocess.run(command, shell=True)
