from kubernetes import client, config
import grpc
import os
import socket
from concurrent import futures
import gossip_pb2
import gossip_pb2_grpc
import json
import time

class Node(gossip_pb2_grpc.GossipServiceServicer):
    def __init__(self, service_name):

        self.pod_name = socket.gethostname()
        self.host = socket.gethostbyname(self.pod_name)
        self.port = '5050'
        self.service_name = service_name

        # Load the topology from the "topology" folder
        topology_folder = "topology"

        # get topology based on model, cluster (or other cluster) and total number of nodes
        self.topology = self.get_topology(os.environ['NODES'], topology_folder)

        # Find neighbors based on the topology (with latency)
        # but not from the real network
        self.neighbor_pods = self._find_neighbors(self.pod_name)
        print(f"{self.pod_name}({self.host}) neighbors: {self.neighbor_pods}", flush=True)

        self.received_messages = set()

        self.gossip_initiated = False
        self.initial_gossip_timestamp = None
        self.neighbor_ip_update = False # no ip addrs for pod neighbors yet

    def get_topology(self, total_replicas, topology_folder, model="Full"):
        """
        Retrieves the number of replicas for the specified StatefulSet using kubectl
        and finds the corresponding topology file in the 'topology' subfolder
        within the current working directory.
        """

        # Get the current working directory
        current_directory = os.getcwd()

        # Construct the full path to the topology folder
        topology_dir = os.path.join(current_directory, topology_folder)

        # get filename based on the number of nodes
        search_str = f'nodes{total_replicas}_'

        # Find the corresponding topology file
        topology_file = None
        for topology_filename in os.listdir(topology_dir):
            if topology_filename.startswith(search_str):
                if model in topology_filename:
                    topology_file = topology_filename
                    break

        # For debugging
        print(f"topology_dir: {topology_dir}", flush=True)
        print(f"topology_folder : {topology_folder }", flush=True)
        print(f"topology_file: {topology_file}", flush=True)

        if topology_file:
            with open(os.path.join(topology_dir, topology_file), 'r') as f:
                return json.load(f)
        else:
            raise FileNotFoundError(f"No topology file found for {total_replicas} nodes.")

    def get_pod_ip(self, pod_name, namespace="default"):
        """
        Retrieves pod ip addr based on the given pod_name
        """
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)
        return pod.status.pod_ip

    def SendMessage(self, request, context):

        """
        Receiving message from other nodes
        and distribute it to others
        """
        message = request.message
        sender_id = request.sender_id
        received_timestamp = time.time_ns()

        # Check for message initiation and set the initial timestamp
        if sender_id == self.pod_name and not self.gossip_initiated:
            self.gossip_initiated = True
            self.initial_gossip_timestamp = received_timestamp
            log_message = (f"Gossip initiated by {self.pod_name}({self.host}) at "
                           f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(received_timestamp / 1e9))}")
            self._log_event(message, sender_id, received_timestamp, None,
                            'initiate', log_message)
            self.gossip_initiated = False  # For multiple tests, need to reset gossip initialization

        # Check for duplicate messages
        elif message in self.received_messages:
            log_message = f"{self.pod_name}({self.host}) ignoring duplicate message: {message} from {sender_id}"
            self._log_event(message, sender_id, received_timestamp, None,'duplicate', log_message)
            return gossip_pb2.Acknowledgment(details=f"Duplicate message ignored by {self.pod_name}({self.host})")

        # Send to message neighbor (that is  not receiving the message yet)
        else:
            self.received_messages.add(message)
            propagation_time = (received_timestamp - request.timestamp) / 1e6
            log_message = (f"{self.pod_name}({self.host}) received: '{message}' from {sender_id}"
                           f" in {propagation_time:.2f} ms ")
            self._log_event(message, sender_id, received_timestamp, propagation_time, 'received', log_message)

        # Gossip to neighbors (only if the message is new)
        self.gossip_message(message, sender_id)
        return gossip_pb2.Acknowledgment(details=f"{self.pod_name}({self.host}) processed message: '{message}'")

    def gossip_message(self, message, sender_id):

        """
        This function objective is to send message to all neighbor nodes.
        """
        # update neighbor ip address (if it is not updated)
        if not self.neighbor_ip_update:
            self.neighbor_pods = self.update_neighbors()

        # Get the neighbor and its ip address
        for neighbor in self.neighbor_pods:
            if neighbor[0] != sender_id: # neighbor name
                target = f"{neighbor[1]}:5050" # neighbor ip address

                # Record the send timestamp
                send_timestamp = time.time_ns()

                with grpc.insecure_channel(target) as channel:
                    try:
                        stub = gossip_pb2_grpc.GossipServiceStub(channel)
                        stub.SendMessage(gossip_pb2.GossipMessage(
                            message=message,
                            sender_id=self.pod_name,
                            timestamp=send_timestamp,
                        ))
                    except grpc.RpcError as e:
                        print(f"Failed to send message: '{message}' to {neighbor[0]}: {e}", flush=True)

    def _find_neighbors(self, node_id):
        """
        Identifies the neighbors of the given node based on the topology (json)
        """

        neighbors = []
        for edge in self.topology['edges']:
            if edge['source'] == node_id:
                neighbors.append((edge['target']))  # Add neighbor as a tuple (from topology)
            elif edge['target'] == node_id:
                neighbors.append((edge['source']))  # Add neighbor as a tuple (from topology)

        return neighbors

    def update_neighbors(self, namespace="default"):
        """
        Returns a list of neighbors for the given node_id along with their IP addresses.
        """
        neighbors_with_ip = []
        for neighbor in self.neighbor_pods:
            ip = self.get_pod_ip(neighbor, namespace)
            neighbors_with_ip.append((neighbor, ip))  # Append (node_id, ip) as a tuple
        self.neighbor_ip_update = True
        return neighbors_with_ip

    def _log_event(self, message, sender_id, received_timestamp, propagation_time, event_type, log_message):
        """Logs the gossip event as structured JSON data."""
        event_data = {
            'message': message,
            'sender_id': sender_id,
            'receiver_id': self.pod_name,
            'received_timestamp': received_timestamp,
            'propagation_time': propagation_time,
            'event_type': event_type,
            'detail': log_message
        }

        # Print both the log message and the JSON data to the console
        print(json.dumps(event_data), flush=True)

    def start_server(self):
        """ Initiating server """
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        gossip_pb2_grpc.add_GossipServiceServicer_to_server(self, server)
        server.add_insecure_port(f'[::]:{self.port}')
        print(f"{self.pod_name}({self.host}) listening on port {self.port}", flush=True)
        server.start()
        server.wait_for_termination()

def run_server():
    service_name = os.getenv('SERVICE_NAME', 'bcgossip')
    node = Node(service_name)
    node.start_server()

if __name__ == '__main__':
    run_server()