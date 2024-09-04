import asyncio
import random
import aiocoap.resource as resource
import aiocoap
from aiocoap import *
import datetime
import json
import sys

"""
TODO
"""


class zookeeper(resource.Resource):
    # constructor
    def __init__(self, id, port_no):
        super().__init__()
        self.id = id
        self.port_no = port_no
        self.time = datetime.datetime.now()
        self.leader = 0
        # list of brokers maintained by zookeeper
        with open("brokers.json", "w+") as f:
            f.truncate()
            f.write("[]")
        print("Zookeeper initialized at %s" % self.time)
    # informs the new leader among brokers

    async def send_leader_message(self, new_leader):
        # creating a client to enable communication
        context = await Context.create_client_context()
        print("This assigns a new leader")
        payload_message = {
            "code": "You're the new leader"
        }
        # sending message to elected leader
        payload = json.dumps(payload_message)
        request = Message(code=aiocoap.PUT, payload=bytes(
            payload, 'utf-8'), uri="coap://localhost:%s/Broker" % new_leader["port"])
        # waiting for an acknowledgement from the broker
        response = await context.request(request).response
        print(response.payload)
        return response

    # leader election function
    def new_leader(self, data):
        # the new leader is elected in random
        new_leader_index = random.randint(0, len(data)-1)
        # data is the information stored in the json converted into key-value pairs
        new_leader = data[new_leader_index]
        print("The new leader is broker ", new_leader["id"])
        data[new_leader_index]["status"] = "leader"
        print(new_leader["port"])
        # returning new leader
        return data, new_leader

    async def render_put(self, request):
        # recives data
        # Payload recieved as a byte string
        bytes_input = request.payload
        # Convert byte string payload to string
        string_input = bytes_input.decode()
        # Convert string into a dictionary format
        message = json.loads(string_input)

        # print("New broker id : %s , Port : %s" % (message['id'],message['port']))
        # print(message["code"])
        if message["code"] == "Subscribe zookeeper":
            try:
                # print("Recieved message from consumer")
                f = open("brokers.json")
                broker_list = json.load(f)
                leader_port_no = 0
                for i in broker_list:
                    if i["status"] == "leader":
                        leader_port_no = i["port"]
                response_input = bytes(str(leader_port_no), 'utf-8')
                return aiocoap.Message(code=aiocoap.CHANGED, payload=response_input)
            except Exception as e:
                print(e)

        elif message['code'] == "Publish zookeeper":
            try:
                # print("Recieved message from consumer")
                f = open("brokers.json")
                broker_list = json.load(f)
                leader_port_no = 0
                for i in broker_list:
                    if i["status"] == "leader":
                        leader_port_no = i["port"]
                response_input = bytes(str(leader_port_no), 'utf-8')
                return aiocoap.Message(code=aiocoap.CHANGED, payload=response_input)
            except Exception as e:
                print(e)
                print("Failed to send message to publisher")

        elif message['code'] == "Send broker information to Leader":
            try:
                broker_list = self.findBrokerList()
                mess = {
                    "arr": broker_list
                }

                response_input = bytes(json.dumps(broker_list), 'utf-8')
                return aiocoap.Message(code=aiocoap.CHANGED, payload=response_input)
            except Exception as e:
                print(e)
        elif message['code'] == "Informing zookeeper":
            # Register broker into zookeepers data
            with open('brokers.json', 'r+') as f:
                data = json.load(f)

                # Assigns broker a role
                if data == []:
                    leader_status = "leader"
                else:
                    leader_status = "follower"

                new_broker_data = {
                    "id": message['id'],
                    "port": str(message["port"]),
                    "status": leader_status
                }

                data.append(new_broker_data)
                f.seek(0)
                json.dump(data, f, indent=4)
                f.truncate()

            response_input = b"New broker registered"
            return Message(code=aiocoap.CHANGED, payload=response_input)

    def findBrokerList(self):
        f = open('brokers.json', "rb")
        data = json.load(f)
        f.close()

        send_list = []
        for broker in data:
            id, port = broker['id'], broker['port']
            send_list.append([id, port])
        return send_list

    async def heartbeat(self):
        while True:
            await asyncio.sleep(5)
            with open('brokers.json', 'r+') as f:
                data = json.load(f)
                if data == []:
                    print("No active brokers")
                else:
                    # Iterate through each broker and check for a heartbeat
                    for idx, broker in enumerate(data):
                        context = await Context.create_client_context()
                        print(broker["port"])

                        request = Message(
                            code=aiocoap.GET, uri="coap://localhost:%s/Broker" % broker["port"])

                        try:
                            response = await context.request(request).response
                        except Exception as e:
                            print('Failed to fetch resource: ')
                            print(e)
                            print("Lost connect to broker %s" % broker["id"])
                            data.pop(idx)
                            if broker["status"] == "leader":
                                print(data)
                                data, leader_info = self.new_leader(data)
                                response = await self.send_leader_message(new_leader=leader_info)
                                print(response.payload)
                        else:
                            print('Heartbeat recieved\nResult: %s \t Payload: %r' % (
                                response.code, response.payload))
                    f.seek(0)
                    json.dump(data, f, indent=4)
                    f.truncate()

    def activate(self):
        print("Activated zookeeper")
        root = resource.Site()
        root.add_resource(['zookeeper'], zookeeper(
            id=self.id, port_no=self.port_no))
        asyncio.Task(aiocoap.Context.create_server_context(
            root, bind=('localhost', self.port_no)))
        asyncio.Task(self.heartbeat())
        asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    # CLAs to assign an ID and a port number
    id = sys.argv[1]
    port_no = int(sys.argv[2])
    # creating zookeeper object and activating it
    new_zookeeper = zookeeper(id, port_no)
    new_zookeeper.activate()
