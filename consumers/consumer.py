import asyncio
import aiocoap.resource as resource
import aiocoap
from aiocoap import *
import datetime
import json
import sys


"""
TODO
- Send topic subscriptions to the leader brokers port
"""


class Consumer(resource.Resource):
    def __init__(self, id, port_no,zookeeper_id=3000):
        super().__init__()
        self.id = id
        self.port_no = port_no

        self.zookeeper_id=zookeeper_id
        self.assignedBroker=port_no
        self.msgs = {}
        # to get previous topic messages
        self.time = datetime.datetime.now()
        print("Consumer %s initialized at %s" % (self.id, self.time))

    async def render_get(self, request):
        str_payload = 'Heartbeat from %s' % self.port_no
        # Convert string payload to bytes
        payload = bytes(str_payload, 'utf-8')
        # print(payload)
        return Message(payload=payload)

    async def render_put(self, request):
        # recives data
        # Payload recieved as a byte string
        bytes_input = request.payload
        string_input = bytes_input.decode()
            # Convert string into a dictionary format
        message = json.loads(string_input)
        if message['code']=="Subscribe zookeeper":
            print("Trying to fetch broker number")
            return Message(code=aiocoap.CHANGED,payload=bytes_input)
        else:
            # Convert byte string payload to string
            print("Given message : %s , Topic : %s" %
                (message['message'], message['topic']))
            return Message(code=aiocoap.CHANGED, payload=bytes_input)

    async def get_messages_broker(self,fromBeginning:bool,topic):
        mess={
            'code':'Recive topic messages',
            'topic':topic,
            'fromBeginning':fromBeginning
        }

        context = await Context.create_client_context()
        payload=bytes(json.dumps(mess),'utf-8')
        req=Message(code=aiocoap.PUT,payload=payload,uri="coap://localhost:%s/Broker" % self.assignedBroker)
        try:
            response=await context.request(req).response
            str_payload = response.payload.decode()
            print(f"Broker has replied with message {str_payload}")
            
        except Exception as e:
            print("Failed to get broker message response")
            print(e)


    async def getBrokerId(self,topic):
        #connect to zookeeper first

        mess={
            "code":"Subscribe zookeeper"
        }
        context = await Context.create_client_context()
        payload=bytes(json.dumps(mess),'utf-8')
        req=Message(code=aiocoap.PUT,payload=payload,uri="coap://localhost:%s/zookeeper" % self.zookeeper_id)
        try:
            response=await context.request(req).response
            str_payload = response.payload.decode()
            leader_broker_port=str_payload
            print(leader_broker_port)
            print(f"Zookeeper has replied with leader_broker_port {leader_broker_port}")
            
        except Exception as e:
            print("Failed to get zookeeper response")
            print(e)
        
        #got leader broker port now get the broker port

        mess={
            "code":"Subscribe broker",
            "id":self.id,
            "topic":topic
        }
        context = await aiocoap.Context.create_client_context()
        payload=bytes(json.dumps(mess),'utf-8')
        req=Message(code=aiocoap.PUT,payload=payload,uri="coap://localhost:%s/Broker" % leader_broker_port)
        try:
            response=await context.request(req).response
            str_payload = response.payload.decode()
            print("Broker port is ",str_payload)
            sub_broker_port=str_payload
            self.assignedBroker=sub_broker_port
            await self.get_messages_broker(True,sys.argv[4])
            return sub_broker_port


        except Exception as e:
            print("Failed to get zookeeper response")
            print(e)

        





    def save_data(self):
        # saves consumer data on local file sysytem just for viewing
        pass

    async def extract_msg(self):
        # assuming messages have been sent in the format [filename,bytestream]
        # this is where we receive the payload, get rid of this while integrating (added to avoid warnings)
        msg = []
        filename = msg[0][-5:].split('_')
        value = msg[1].decode()
        self.msgs[filename[0]] = {filename[1]: value}
        if self.msgs[filename[0]].keys == range(filename[2]):
            decoded = ''
            for i in self.msgs[filename[0]].keys:
                decoded += self.msgs[filename[0]][i]
            print(decoded)

    async def heartbeat(self):
        while True:
            await asyncio.sleep(5)
            context = await Context.create_client_context()

            request = Message(
                code=aiocoap.GET, uri="coap://localhost:%s/Broker" % sys.argv[3])

            try:
                response = await context.request(request).response
            except Exception as e:
                print('Failed to fetch resource:')
                print(e)
            else:
                print('Heartbeat recieved\nResult: %s \t Payload: %r' %
                      (response.code, response.payload))

    def activate(self,topic):
        root = resource.Site()
        root.add_resource(['Consumer'], Consumer(
            id=self.id, port_no=self.port_no))
        asyncio.Task(aiocoap.Context.create_server_context(
            root, bind=('localhost', self.port_no)))
        asyncio.Task(self.heartbeat())
        asyncio.Task(self.getBrokerId(topic))
        asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    id = sys.argv[1]
    port_no = int(sys.argv[2])
    to_port_no = int(sys.argv[3])
    topic = sys.argv[4]

    print(id, port_no, to_port_no)
    new_consumer = Consumer(id=id, port_no=port_no)
    # new_consumer.send_message(1,1,to_port_no=port_no)
    new_consumer.activate(topic=topic)
