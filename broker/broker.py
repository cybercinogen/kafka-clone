import asyncio
import aiocoap.resource as resource
import aiocoap
from aiocoap import *
import datetime
import json
import sys
import os
import logging

"""
TODO
- Send partition dates along with partitions
"""

# some helper functions


async def message_send(payload, route, port):
    context = await Context.create_client_context()
    payload_msg = bytes(json.dumps(payload), 'utf-8')
    try:
        request = Message(code=aiocoap.PUT, payload=payload_msg,
                          uri="coap://localhost:%s/%s" % (port, route))
        response = await context.request(request).response
    except Exception as e:
        print("An error occurred while sending message")
        print(e)
    return response


class helperFunctions():
    def tree_printer(root):
        # fs_list=[[folder_name],[file_name]]
        folder_names = []
        folder_names.append(root)
        file_names = []

        for root, dirs, files in os.walk(root):
            for d in dirs:
                folder = os.path.join(root, d)
                folder_names.append(folder)
            for f in files:
                file = os.path.join(root, f)
                file_names.append(file)
        fs_list = [folder_names, file_names]
        return fs_list

    def filesplitter(bytearray: bytes, partition_size):
        counter = 0
        len_function = len
        arr = []
        while counter < len_function(bytearray)-partition_size:
            arr.append(bytearray[counter:counter+partition_size])
            counter += partition_size
        arr.append(bytearray[counter:])
        return arr


class Broker(resource.Resource):
    def __init__(self, id, port_no, path_to_fs, isLeader=0):
        super().__init__()
        self.isLeader = isLeader
        self.id = id
        self.port_no = port_no
        self.path_to_fs = path_to_fs
        self.broker_data = []
        # creating broker filestructure
        os.system(f"rm -rf {os.path.join(self.path_to_fs,self.id)}")
        self.create_fs()

        # creating logfile
        logging.basicConfig(filename=os.path.join(path_to_fs, id, f"{id}.log"),
                            format='%(asctime)s %(message)s',
                            filemode='w')

        # to get previous topic messages
        self.time = datetime.datetime.now()
        print("Broker initialized at %s" % self.time)

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.info("Broker Initialized.")
        self.logger.info("Broker FS Created")
        # Logger commands
        # logger.debug("Harmless debug Message")
        # logger.info("Just an information")
        # logger.warning("Its a Warning")
        # logger.error("Did you try to divide by zero")
        # logger.critical("Internet is down")

        # maintaining file structure information
        self.fs = helperFunctions.tree_printer(os.path.join(path_to_fs, id))
        self.partition_no = {}
        self.partition_date = {}

        # filestructure of node
        self.fileStructure = helperFunctions.tree_printer(
            os.path.join(self.path_to_fs, self.id))

        #manganging topic broker, customer relationship
        #"broker_portno':{topic:[list of consumers assigned],consumer_count=int}
        # self.broker_assgin={"testbrokerno":{"testtopic":[1223,2323,43434],"consumer_count":3}}
        self.broker_assign = {}

    def create_fs(self):
        folder_path = os.path.join(self.path_to_fs, self.id)
        os.mkdir(folder_path)


    #Error handle this - Multiple brokers can be assigned the same topic - make it so that only one topic is assigned to one broker
    def assignTopicToBroker(self,topic,consumer_id):
        print("Calling function")
        try:
            min_load=99999
            min_port=None
            with open('broker_list.json','r+') as f:
                broker_list = json.load(f)
                print(broker_list)
                for i in broker_list:
                    # print(broker_list[i]['consumer_count'])
                    if broker_list[i]['consumer_count']<min_load:
                        min_load=broker_list[i]['consumer_count']
                        min_port=i

                if topic not in broker_list[min_port]:
                    broker_list[min_port][topic]=[]

                broker_list[min_port][topic].append(consumer_id)
                broker_list[min_port]['consumer_count']+=1
                print(broker_list)
                self.broker_assign = broker_list
                f.seek(0)
                json.dump(broker_list,f,indent=4)
                f.truncate()
                # print(min_port)
                #returns broker with min load 
                return min_port
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(e,'\n',exc_type, fname, exc_tb.tb_lineno)


    async def render_get(self, request):
        str_payload = 'Heartbeat from %s' % self.port_no
        # Convert string payload to bytes
        payload = bytes(str_payload, 'utf-8')
        # print(payload)
        return Message(payload=payload)

    async def render_put(self, request):
        # Recives data
        # Payload recieved as a byte string
        bytes_input = request.payload
        # Convert byte string payload, '../fs/001/apple/0_0', '../fs/001/apple/1_1', '../fs/001/apple/2_2',  to string
        string_input = bytes_input.decode()
        # Convert string into a dictionary/json format
        message = json.loads(string_input)
        # print(message)
        if message["code"] == 'Gimme files':
            # Convert list to a string
            self.fileStructure = helperFunctions.tree_printer(
                os.path.join(self.path_to_fs, self.id))
            worker_filestructure, worker_files = self.fileStructure
            fs = ' '.join([str(elem) for elem in worker_files])
            payload = bytes(fs, 'utf-8')
            return Message(code=aiocoap.CHANGED, payload=payload)
            # change the names of leader file to worker code
        elif message["code"] == "Recieve files from leader broker":
            print(message["file name"], " --> ", message["content"])
            file_split = message["file name"].split('/')
            file_split[2] = self.id
            new_file_path = "/".join(file_split)
            print(new_file_path)
            self.save_data_file(bytes(message["content"],'utf-8'),file_split[3],new_file_path)
            return Message(code=aiocoap.CHANGED,payload=b'File recieved')

        elif message["code"] == "You're the new leader":
            print("I'm the leader now")
            self.isLeader = 1
            return Message(code=aiocoap.CHANGED, payload=b'New leader assigned')

        elif message["code"] == "Subscribe broker":
            print("Subscribe broker condition")
            broker_port=self.assignTopicToBroker(message["topic"],message['id'])
            #assignbroker(topic)
            brokerassigned=broker_port
            response_input=bytes(brokerassigned,'utf-8')
            return Message(code=aiocoap.CHANGED,payload=response_input)
            
        elif message["code"] == "Producer message":
            print("Given message : %s , Topic : %s" %
                  (message['message'], message['topic']))
            bytes_message = bytes(message['message'], 'utf-8')
            self.save_data(bytes_message, 6, message["topic"])
            self.logger.info("Message from producer id %s, Message: %s, Topic: %s" % (
                message['id'], message['message'], message['topic']))
            return Message(code=aiocoap.CHANGED, payload=b'Message from producer recieved')
        
        elif message["code"] == "Sending partion date":
            print("Reciving partion timings from LEADER")
            arr=message["arr"]
            self.partition_date=arr
            self.logger.info("Message from producer id %s, Message: %s, Topic: %s" % (
                message['id'], message['message'], message['topic']))
            return Message(code=aiocoap.CHANGED, payload=b'Partition from Leader recieved')

        elif message["code"] == "Recive topic messages":
            try:
                print("Sending topic messages to consumer")
                topic=message["topic"]
                mess= self.send_message_to_consumer(topic,message['fromBeginning'])
                payload=bytes(str(mess),'utf-8')
                print(payload)
                self.logger.info(f"Sending message to consumer : {payload}")
                return Message(code=aiocoap.CHANGED, payload=payload)
            except Exception as e:
                print(e)

    async def send_partition_date(self,list_of_worker_ports):
        if self.isLeader:
            mess={
                "code":"Sending partion date",
                "arr":self.partition_date
            }
            payload = json.dumps(mess)
            # Convert string to bytes string
            payload_bytes = bytes(payload, 'utf-8')

            for port_no in list_of_worker_ports:
                context = await Context.create_client_context()
                req = Message(code=aiocoap.PUT, payload=payload_bytes,
                                uri="coap://localhost:%s/Broker" % port_no)
                try:
                    response = await context.request(req).response
                    # print(response.payload)
                    str_payload = response.payload.decode()
                    print("Worker replied with OK -Recieved file timings")

                except Exception as e:
                    print('Failed to fetch resource: ')
                    print(e)

    def save_data_file(self, bytearray, topic, name_file):
        try:
            file_name = os.path.join(self.path_to_fs, self.id, topic)
            if os.path.exists(file_name):
                pass
            else:
                os.mkdir(file_name)
            create_file = os.path.join(name_file)
            if os.path.exists(create_file):
                pass
            else:
                f = open(create_file,'wb')
                f.write(bytearray)
                f.close()
                self.logger.info(f'partition Created at {create_file}')
                self.fileStructure = helperFunctions.tree_printer(
                    os.path.join(self.path_to_fs, self.id))
                self.partition_no[topic] += 1
        except Exception as e:
            print(e)

    def save_data(self, bytearray, partitionsize, topic):
        try:
            file_name = os.path.join(self.path_to_fs, self.id, topic)
            print(file_name)
            os.mkdir(file_name)
        except:
            print("error creating topic folder")
        finally:

            if topic not in self.partition_no:
                self.partition_no[topic] = 0

           # saves Broker data on local file sysytem just for viewing
            partitions = helperFunctions.filesplitter(
                bytearray=bytearray, partition_size=partitionsize)
            counter = 0
            for i in partitions:
                to_file = os.path.join(self.path_to_fs, self.id, topic, str(
                    self.partition_no[topic])+"_"+str(counter)+"_"+str(len(partitions)-1))
                f = open(to_file, 'wb')
                f.write(i)
                f.close()

                if topic not in self.partition_date:
                    self.partition_date[topic]=[]
                
                
                self.partition_date[topic].append(to_file)
                self.logger.info(f'partition Created at {to_file}')
                counter += 1
                self.fileStructure = helperFunctions.tree_printer(
                    os.path.join(self.path_to_fs, self.id))
            self.partition_no[topic] += 1
    # Sends file structure from leader to worker nodes

    def send_message_to_consumer(self,topic,fromBeginning):
        filenames=[self.partition_date[topic][-1]]
        if fromBeginning:
            filenames=self.partition_date[topic]
        
        payload=[]
        # print(self.partition_date[topic])
        for filename in filenames:
            file_split = filename.split('/')
            file_split[2] = self.id
            new_file_path = "/".join(file_split)

            try: 
                f=open(new_file_path,'rb')
                data=f.read()
                payload.append(data)

            except Exception as e:
                print("Unable to open file")
        # print(payload)
        return payload


        



    async def sendFileStructure(self, to_port):
        tree = helperFunctions.tree_printer(
            os.path.join(self.path_to_fs, self.id))
        tree = bytes(tree)
        # print(tree)
        request = aiocoap.Message(
            code=aiocoap.PUT, payload=self, uri="coap://localhost")

    async def replicateFiles(self, list_of_worker_portno: list):
        while True:
            await asyncio.sleep(10)
            if self.isLeader == 1:
                leader_filestructure, leader_files = self.fileStructure
                f = open('broker_list.json','r+')
                data = json.load(f)
                f.close()
                print("Keys: ",data.keys())
                for port_no in data:
                    print(port_no)
                    payload_message = {
                        "code": "Gimme files"
                    }
                    payload = json.dumps(payload_message)
                    # Convert string to bytes string
                    payload_bytes = bytes(payload, 'utf-8')
                    #  whatever files the baccha(port) has list
                    context = await Context.create_client_context()
                    req = Message(code=aiocoap.PUT, payload=payload_bytes,
                                  uri="coap://localhost:%s/Broker" % port_no)
                    try:
                        response = await context.request(req).response
                        # print(response.payload)
                        str_payload = response.payload.decode()
                        worker_files = str_payload.strip('][').split(' ')

                        # print(worker_files)
                        await self.sendFile(leader_files, worker_files, port_no)

                    except Exception as e:
                        print('Failed to fetch resource: ')
                        print(e)

    async def sendFile(self, leader_files, worker_files, port_no):
        context = await Context.create_client_context()


        for i in worker_files:
            file_split = i.split('/')
            file_split[2] = self.id
            new_file_path = "/".join(file_split)
            i=new_file_path
            print(new_file_path)

        files_to_send = [
            i for i in leader_files if i not in worker_files]
        for i in files_to_send:
            # Ignore sending log files
            if i[-4:] == ".log":
                continue

            f = open(i, 'rb')
            file_bytes = f.read()
            f.close()
            try:
                # print('File bytes ',file_bytes)
                payload_message = {
                    "code": "Recieve files from leader broker",
                    "content": file_bytes.decode(),
                    "file name": i
                }
                # print(payload_message)

                payload = json.dumps(payload_message)
                # Convert string to bytes string
                payload_bytes = bytes(payload, 'utf-8')
                # print(payload_bytes)
                new_req = Message(
                    code=aiocoap.PUT, payload=payload_bytes, uri="coap://localhost:%s/Broker" % port_no)
            except Exception as e:
                print(e)
            response = await context.request(new_req).response
            print(response.payload)
            # bytes_to_send=bytes([i,file_bytes])

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

    async def get_brokers_info(self):
        mess = {
            "code": "Send broker information to Leader"
        }
        payload = bytes(json.dumps(mess), 'utf-8')
        context = await aiocoap.Context.create_client_context()

        try:
            req = aiocoap.Message(code=aiocoap.PUT, payload=payload,
                                  uri="coap://localhost:%s/zookeeper" % sys.argv[3])
            response = await context.request(req).response
            res_payload = response.payload.decode()
            data = json.loads(res_payload)
            print(data)
            for i in data:
                print("Check")
                id, port_no = i[0], i[1]
                # print(id,port_no)
                self.broker_assign[port_no] = {}
                self.broker_assign[port_no]['consumer_count'] = 0
                with open('broker_list.json','r+') as f:
                    f.seek(0)
                    json.dump(self.broker_assign,f,indent=4)
                    f.truncate()
            print(f"Got brokers info from LEADER -{data}")
        except Exception as e:
            print("Unable to find ZOOKEEPER message")
            print(e)

    async def inform_zookeeper(self):
        context = await Context.create_client_context()

        payload_message = {
            "code": "Informing zookeeper",
            "id": self.id,
            "port": self.port_no
        }

        # Convert dict to string
        payload = json.dumps(payload_message)
        # Convert string to bytes string
        payload_bytes = bytes(payload, 'utf-8')

        request = Message(code=aiocoap.PUT, payload=payload_bytes,
                          uri="coap://localhost:%s/zookeeper" % sys.argv[3])

        try:
            response = await context.request(request).response
        except Exception as e:
            print('Failed to fetch resource:')
            print(e)
        else:
            # print('Heartbeat recieved\nResult: %s \t Payload: %r'%(response.code, response.payload))
            print("Zookeeper response : %s", response.payload)


def activate(broker_class, port):
    root = resource.Site()
    # root.add_resource(['Broker'], Broker(id=id,port_no=port,path_to_fs=path,isLeader=leader))
    root.add_resource(['Broker'], broker_class)
    asyncio.Task(aiocoap.Context.create_server_context(
        root, bind=('localhost', port))),
    asyncio.Task(broker_class.inform_zookeeper())
    # asyncio.Task(self.heartbeat())
    # stri = "This is a string"
    # b = bytes(stri, encoding="utf-8")
    # print(b)
    # print(broker_class.save_data(b, 6, "apple"))
    # print(broker_class.save_data(b, 6, "ball"))
    # print(broker_class.save_data(b, 6, "cat"))
    # print(broker_class.save_data(b, 6, "draken-kun-uwu"))
    # print(broker_class.save_data(b, 6, "e"))
    asyncio.Task(broker_class.replicateFiles([3001, 3002, 3003]))
    asyncio.Task(broker_class.get_brokers_info())
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    id = sys.argv[1]
    port_no = int(sys.argv[2])
    to_port_no = int(sys.argv[3])
    isLeader = int(sys.argv[4])

    newBroker = Broker(id=id, port_no=port_no,
                       path_to_fs='../fs', isLeader=isLeader)
    activate(newBroker, port_no)
    # stri="This is a string"
    # b=bytes(stri,encoding="utf-8")
    # print(b)
    # print(newBroker.save_data(b,6,"test_topic"))

    # print(helperFunctions.tree_printer('../fs'))
