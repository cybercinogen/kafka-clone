# client_put.py
import asyncio
import json
import aiocoap
from aiocoap import *
import sys

"""
TODO
- Ask zookeeper for the leaders port number every few seconds
"""


class Producer():
    def __init__(self, id, port_no) -> None:
        self.id = id
        self.port_no = port_no
        print("Producer %s initialized" % self.id)

    async def getBrokerId(self):
        # Connect to zookeeper first
        try:
            context = await Context.create_client_context()
            message = {
                "code": "Publish zookeeper"
            }
            payload = bytes(json.dumps(message), 'utf-8')
            req = Message(code=aiocoap.PUT, payload=payload,
                          uri="coap://localhost:%s/zookeeper" % sys.argv[3])
            response = await context.request(req).response
            str_payload = response.payload.decode()
            leader_broker_port = str_payload
            print(leader_broker_port)
            print(
                f"Zookeeper has replied with leader_broker_port {leader_broker_port}")

            return leader_broker_port
        except Exception as e:
            print("Failed to get zookeeper response")
            print(e)

    async def create_message(self):
        try:
            message = input("Enter a message: ")
            topic = input("Enter topic: ")
            payload_message = {
                "code": "Producer message",
                "id": self.id,
                "message": message,
                "topic": topic
            }

            # Convert dict to string
            payload = json.dumps(payload_message)
            # Convert string to bytes string
            payload_bytes = bytes(payload, 'utf-8')
            return payload_bytes
        except KeyboardInterrupt:
            raise SystemExit

    async def send_message(self):
        context = await Context.create_client_context()
        payload_bytes = await self.create_message()
        # message = input("Enter a message: ")
        # topic = input("Enter topic: ")
        # payload_message = {
        #     "code": "Producer message",
        #     "id": self.id,
        #     "message": message,
        #     "topic": topic
        # }

        # # Convert dict to string
        # payload = json.dumps(payload_message)``
        # # Convert string to bytes string
        # payload_bytes = bytes(payload, 'utf-8')
        # print(payload_bytes)
        # Send payload as a byte string
        try:
            leader_port = await self.getBrokerId()
            request = Message(code=aiocoap.PUT, payload=payload_bytes,
                              uri="coap://localhost:%s/Broker" % leader_port)

            response = await context.request(request).response
            print('Response: %s\n%r' % (response.code, response.payload))
        except Exception as e:
            print(e)


# async def main(new_producer):
#    await new_producer.send_message()

if __name__ == "__main__":
    # CLA to receive producer ID and port number
    id = sys.argv[1]
    port_no = sys.argv[2]
    # Zookeeper port number - remains common across all producers
    zookeeper_port = sys.argv[3]
    # initialise object
    new_producer = Producer(id, port_no)
    # topic and message input from terminal
    # message = input("Enter a message: ")
    # topic = input("Enter topic: ")

    # asyncio.Task(new_producer.send_message())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(new_producer.send_message())
    loop.run_forever()
    # asyncio.get_event_loop().run_forever()
