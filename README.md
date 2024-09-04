

A kafka clone made with python .  
Given below is documentation for the various classes and functions implemented for this project.

## Classes

---

### Producer

Class called in order to perform publish operations (send message to brokers)

#### functions

- `getBrokerId`

  - communicates with the zookeeper
  - receives the port number of the leader broker to perform future publish operations

- `create_message`

  - async function -> works on event loop
  - receives user input of topic and message
  - converts into json, which is converted into a bytestream
  - returns the payload bytestream

- `send_message`

  - Sends the json payload to an assigned port (leader port)
  - payload consists of a given message and topic which is generated in `create_message`

---

### Consumer

Class is assigned with a unique ID and port number along with the heartbeat for the broker

- Send heartbeat = Requests for a heartbeat signal at the assigned port

- render_put = Recieves inputs with the message and topic while activated

- save_data = saves consumer data on local file sysytem just for viewing

- render_get = Sends a heartbeat to the server that pings it using a get request

- Activate = Starts the server to recieve messages and heartbeats

### Broker

- Leader func

  - Receives messages from pub
  - Create pratitions for data
  - Replicate partitions to each broker
  - Handles consume and publish

- Normal brokers
  - Consume operations
  - Recieve all partitions from da leader

---

### Mini - Zookeeper

Class that monitors health of all brokers and handles broker failure, including leader re-election.
A json file is maintained with a list of brokers and their information such as ID, port number and role.

#### functions

- `send_leader_message`

  - creates a client to enable communication
  - sends a json payload informing the newly elected leader of its new role

- `new_leader`

  - elects a new leader broker
  - election algorithm -> random selection
  - json file containing broker information is updated with the information of the new leader

- `render_put`

  - performs request-response based communication
  - message codes determine the payload being sent, such as messages declaring the initialisation of a new broker

- `findBrokerList`

  - generates a list of brokers with their information
  - reads and converts the json file to a list

- `heartbeat`

  - recieves heartbeats from all brokers
  - raises alerts on broker failure
  - on failure of leader broker, fires up the leader election protocol

- `activate`

  - creates a coap server
  - server context continuously receives heartbeat

https://excalidraw.com/#room=591784172c37016a87c4,N0D42BUTVJRqdbaJBGzJ0g

---

# TODOs

- Distribute messages between brokers
- Topic subscriptions
- Send to consumers
- Make sure everything runs
- Fault tolerance?

# Done

- Producer

  - Sends messages to broker
  - Able to get the leader port from the zookeeper

- Consumer

  - Return heartbeat
  - Get leader port from zookeeper
  - Recieve a message and topic

- Broker

  - Logging
  - Assign topics
  - Send and recieve files between brokers and leader
  - Recieve messages from producer
  - Send broker heartbeats to zookeeper
  - Broker info from leader
  - sending heartbeat to the zookeeper

- Zookeeper
  - Send a list of brokers to the leader
  - Heartbeat
  - Reassigning leaders
