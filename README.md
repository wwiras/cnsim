## Cloud-Native Simulation Framework for Gossip Protocol: Modeling and Analyzing Network Dynamics
This code is for PLOS One article entitled *Cloud-Native Simulation Framework for Gossip Protocol: Modeling
and Analyzing Network Dynamics*.

**A guide to deploy and utilizing a cloud-native simulation framework for studying gossip protocol 
dynamics in distributed networks.**

### Simulator Overview
This simulator leverages Google Cloud Platform services to provide a scalable and flexible environment for 
modeling and analyzing gossip protocols. The architecture comprises:

* **Google Kubernetes Engine (GKE):** For deploying and managing the distributed network nodes, simulating 
gossip activity. GKE allows for easy scaling and management of the simulation environment.
* **Google BigQuery:** For storing and querying simulation data, enabling efficient data analysis. BigQuery's 
serverless architecture allows for fast and cost-effective analysis of large datasets.
* **Google Colab:** For data visualization and in-depth analysis of simulation results. Colab provides a 
free and accessible environment for Python-based data analysis.
* **python3:** *python* language is used for gossip scripting, data analysis and data virtualization.  

### Implementation Steps

#### Step 1 - Develop grpc communication protocol (using python3)
gRPC is used for inter-node communication, providing efficient and reliable message passing. gRPC is a
high-performance Remote Procedure Call (RPC) framework that allows nodes to communicate as if they 
were calling local functions. The *gossip.proto* file defines the communication interface, specifying 
the message structure and service definitions. This file is then compiled (with command below) and 
generates two *python* files (python classes) in the same directory:
* *gossip_pb2.py*: Contains the *python* classes for your protocol buffer messages (GossipMessage, Acknowledgment).
* *gossip_pb2_grpc.py*: Contains the *python* classes for your gRPC service (GossipServiceServicer, GossipServiceStub).
```python
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. gossip.proto
```

#### Step 2: Gossip Script (Direct Mail Gossip)
The simulator implements a "Direct Mail Gossip" protocol, where nodes directly send messages 
to their neighbors. This is a basic form of gossip protocol, where each node maintains a list of 
its neighbors and forwards messages to them.

- *start.py*: initiates the gossip process by sending a message to the node itself. This simulates a 
node originating a message.
- *node.py*: acts as a gRPC server, receiving and propagating messages to neighboring nodes. It 
listens for incoming messages and, upon receipt, forwards them to the nodes listed in its 
neighbor list.
```shell
# Initiate gossip with the message "Hello, Gossip!" example
python start.py --message "Hello, Gossip!"
```
> **_NOTE:_**  In this simulator, the message will be using a unique ID for easy filtering. Example: '4abf-cubaan50-1'
> This mean this test is for 50 nodes and '-1' as the first test cycle

#### Step 3: Docker Image Creation and Deployment
A Docker image (wwiras/cnsim:v1) is built by running the docker build command at *cnsim* root 
folder and pushing it to Docker Hub. This will ease deployment on GKE. The docker image name and tag are 
flexible. It does not have to be the same as below example.
```shell
$ docker build -t wwiras/cnsim:v1 .
$ docker push wwiras/cnsim:v1
```
#### Step 4: GKE Deployment and Gossip Test
The *automate.py* script automates the deployment (using *helm install* command) of 
the simulator on GKE using a *Deployment*. Once *Deployment* 
is ready, gossip will be initiated by one pod to its neighbor and so on. During this gossip, 
*fluentd* collects logs from each pod and sends them to Google Cloud Logging.  
After the simulation completed, the *helm uninstall* command is executed to bring down all 
*Deployment*. Refer automate.py script for more detail.
```shell
# gossip automation script
python automate.py --num_nodes 10 --num_tests 10
```

#### Step 5: Data Collection and Extraction
Create a dataset for this simulator in BigQuery. Then, create a log "sink" so that all related logs (of this simulator)
are pushed (routed) to the previously created dataset. All related data for each gossip test is filtered based on message 
unique content (UUID). An example of this filter (select statement) is as follows.
```SQL
SELECT jsonPayload.sender_id,jsonPayload.receiver_id, jsonPayload.message, jsonPayload.event_type,
jsonPayload.received_timestamp, jsonPayload.propagation_time,jsonPayload.detail

FROM 
  `stoked-cosine-415611.bcgossip_dataset5.stdout_*`
WHERE 
  _TABLE_SUFFIX BETWEEN '20250325' AND '20250326'

-- 50 nodes
AND jsonPayload.message like  '4abf-cubaan50-%'

LIMIT 1000000
```
From here, save the result to a *.csv file and store it in a google drive for data analysis (in Step 6).

#### Step 6: Data Analysis and Virtualization
Open new Google Colab and point it to the google drive where all the *.csv files have been saved (from Step 6). Execute
data cleaning, analysis and virtualization here. All data and steps for data analysis / virtualization for 
all tests (distribution, bandwidth, geographical and memory) are shown
 [here](https://github.com/wwiras/cnsim).

> **_NOTE:_**  Demo on this simulator can be found [here](https://drive.google.com/file/d/1jEkvELt-3xkGZ5EpXYik6g0AZV26JmQN/view?usp=drive_link).
