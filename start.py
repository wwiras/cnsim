import grpc
import argparse
import time
import socket
import gossip_pb2
import gossip_pb2_grpc
from kubernetes import client, config


def get_pod_ip(pod_name, namespace="default"):
    """Fetches the IP address of a pod in the specified namespace."""
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)
    return pod.status.pod_ip

def send_message_to_self(message):
    """Sends a message to the current pod (itself)."""
    pod_name = socket.gethostname()
    print(f"pod_name={pod_name}", flush=True)
    pod_ip = get_pod_ip(pod_name)
    print(f"pod_ip={pod_ip}", flush=True)
    target = f"{pod_ip}:5050"
    print(f"target={target}", flush=True)

    with grpc.insecure_channel(target) as channel:
        stub = gossip_pb2_grpc.GossipServiceStub(channel)
        print(f"Sending message to self ({pod_name}, {pod_ip}): '{message}'", flush=True)
        response = stub.SendMessage(gossip_pb2.GossipMessage(
            message=message,
            sender_id=pod_name,
            timestamp=time.time_ns()
        ))
        print(f"Received acknowledgment: {response.details}", flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Send a message to self (the current pod).")
    parser.add_argument('--message', required=True, help="Message to send")
    args = parser.parse_args()
    send_message_to_self(args.message)