"""
This script is used to create a fully connected network topology.
"""
import networkx as nx
import json
import os
from datetime import datetime
import argparse

def set_network_mapping(graph, number_of_nodes):
    """
    Renaming nodes based on gossip-statefulset
    """

    # Rename nodes
    mapping = {i: f"gossip-statefulset-{i}" for i in range(number_of_nodes)}
    network = nx.relabel_nodes(graph, mapping)

    return network

def construct_connected_network(number_of_nodes):
    """
    Construct a fully connected network topology.
    Input:
    number_of_nodes: total number of nodes in the network
    Output:
    graph object with a fully connected network topology
    """

    # Create a complete graph
    complete_graph = nx.complete_graph(number_of_nodes)
    print(f"Graph: {complete_graph}")

    return complete_graph

def save_topology_to_json(graph, prob):
    """
    Saves the network topology to a JSON file.

    Args:
    graph: The NetworkX graph object.
    filename: (Optional) The name of the JSON file to save.
    """

    # Get current date and time + second
    type = "Full"
    now = datetime.now()
    dt_string = now.strftime("%b%d%Y%H%M%S")  # Format: Dec232024194653
    filename = f"nodes{len(graph)}_{dt_string}_{type}{prob}.json"

    # Create directory if it doesn't exist
    output_dir = "topology"
    os.makedirs(output_dir, exist_ok=True)

    # prepare topology in json format
    graph_data = nx.node_link_data(graph, edges="edges")
    graph_data["total_edges"] = graph.number_of_edges()
    graph_data["total_nodes"] = graph.number_of_nodes()
    graph_data["model"] = type
    file_path = os.path.join(output_dir, filename)
    with open(file_path, 'w') as f:
        json.dump(graph_data, f, indent=4)
    print(f"Topology saved to {filename}")

def confirm_save(graph, others):
    save_graph = input("Do you want to save the graph? (y/n): ")
    if save_graph.lower() == 'y':
        # Save the topology to a JSON file
        save_topology_to_json(graph, others)

if __name__ == '__main__':

    # Getting input to generate a fully connected network topology
    parser = argparse.ArgumentParser(description="Generate a fully connected network topology")
    parser.add_argument('--nodes', required=True, help="Total number of nodes for the topology")
    # Add the optional argument with a default value of False
    parser.add_argument('--save', action='store_true', help="Save new topology to json(default: False)")
    args = parser.parse_args()

    # Using Fully Connected Model
    number_of_nodes = int(args.nodes)
    graph = construct_connected_network(number_of_nodes)

    # Set network mapping
    if graph:
        # Remapping graph network
        network = set_network_mapping(graph, number_of_nodes)

        # Get nodes and edge info
        network.total_edges = network.number_of_edges()
        network.total_nodes = network.number_of_nodes()

        # Confirmation on creating json (topology) file
        confirm_save(network, 1.0)  # Probability is always 1.0 for fully connected graphs
        print(f"Fully connected network model is SUCCESSFUL ! ....")
    else:
        print(f"Fully connected network model is FAIL ! ....")