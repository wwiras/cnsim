import os
import argparse
import re
import subprocess
import sys
import traceback
import time
import uuid
import select


class Test:
    # def __init__(self,num_test,cluster):
    def __init__(self, num_tests, num_nodes):

        # Getting test details
        self.num_tests = num_tests
        self.num_nodes = num_nodes
        print(f"self.num_test = {self.num_tests}", flush=True)
        print(f'self.num_nodes = {self.num_nodes}', flush=True)

        # getting current folder
        self.current_directory = os.getcwd()
        print(f"self.current_directory = {self.current_directory}", flush=True)

        # getting folder (random or cluster)
        self.topology_folder = os.path.join(self.current_directory,"topology")
        print(f"self.topology_folder = {self.topology_folder}", flush=True)

        # list of files to test (from json filename)
        self.filename = ''
        self.filepath = self.getTopologyFile()
        print(f"self.filepath = {self.filepath}", flush=True)
        print(f"self.filename = {self.filename}", flush=True)

    def getTopologyFile(self):
        """
        Returns a single JSON file for a given directory,
        filtering by the exact number of nodes.
        """
        topology_file = ''
        # Create a regex pattern to match the exact number of nodes
        search_pattern = re.compile(rf'^nodes{self.num_nodes}_.*\.json$')

        try:
            # Find the corresponding topology file
            for topology_filename in os.listdir(self.topology_folder):
                if search_pattern.match(topology_filename):
                    self.filename = topology_filename
                    topology_file = os.path.join(self.topology_folder, topology_filename)
                    break

        except FileNotFoundError:
            print(f"File not found for {self.num_nodes} nodes topology", flush=True)

        return topology_file

    def run_command(self, command, full_path=None):
        """
        Runs a command and handles its output and errors.

        Args:
            command: A list representing the command and its arguments.
            full_path: (Optional) The full path to a file being processed
                       (used for informative messages in case of 'apply' commands).

        Returns:
            A tuple (stdout, stderr) if the command succeeds.
            A tuple (None, stderr) if the command fails.
        """
        try:
            result = subprocess.run(command, check=True, text=True, capture_output=True)

            # If full_path is provided (likely for 'apply' commands), provide more informative output
            if full_path:
                if 'unchanged' in result.stdout or 'created' in result.stdout:
                    print(f"{full_path} applied successfully!", flush=True)
                elif 'deleted' in result.stdout:
                    print(f"{full_path} deleted successfully!", flush=True)
                else:
                    print(f"Changes applied to {full_path}:", flush=True)
                    print(result.stdout, flush=True)

            print(f"result.stdout: {result.stdout}", flush=True)
            print(f"result.stderr: {result.stderr}", flush=True)
            return result.stdout, result.stderr
        except subprocess.CalledProcessError as e:
            if full_path:
                print(f"An error occurred while applying {full_path}.", flush=True)
            else:
                print(f"An error occurred while executing the command.", flush=True)
            print(f"Error message: {e.stderr}", flush=True)
            traceback.print_exc()
            sys.exit(1)
        except Exception as e:
            if full_path:
                print(f"An unexpected error occurred while applying {full_path}.", flush=True)
            else:
                print(f"An unexpected error occurred while executing the command.", flush=True)
            traceback.print_exc()
            sys.exit(1)

    def wait_for_pods_to_be_ready(self, namespace='default', expected_pods=0, timeout=1000):
        """
                Waits for all pods in the specified namespace to be down
                by checking every second until they are terminated or timeout is reached.
        """
        print(f"Checking for pods in namespace {namespace}...", flush=True)
        start_time = time.time()
        get_pods_cmd = f"kubectl get pods -n {namespace} --no-headers | grep Running | wc -l"

        while time.time() - start_time < timeout:
            try:
                result = subprocess.run(get_pods_cmd, shell=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

                # Check for "No resources found" in the output
                # print(f"result {result}",flush=True)
                running_pods = int(result.stdout.strip())
                if running_pods >= expected_pods:
                    print(f"All {expected_pods} pods are up and running in namespace {namespace}.", flush=True)
                    return True  # Pods are down
                else:
                    print(f" {running_pods} pods are up for now in namespace {namespace}. Waiting...", flush=True)

            except subprocess.CalledProcessError as e:
                print(f"Error checking for pods: {e.stderr}", flush=True)
                return False  # An error occurred

            time.sleep(1)  # Check every second

        print(f"Timeout waiting for pods to terminate in namespace {namespace}.", flush=True)
        return False  # Timeout reached

    def wait_for_pods_to_be_down(self, namespace='default', timeout=1000):
        """
        Waits for all pods in the specified namespace to be down
        by checking every second until they are terminated or timeout is reached.
        """
        print(f"Checking for pods in namespace {namespace}...", flush=True)
        start_time = time.time()
        # get_pods_cmd = f"kubectl get pods -n {namespace}"
        get_pods_cmd = f"kubectl get pods -n {namespace} --no-headers | grep Terminating | wc -l"

        while time.time() - start_time < timeout:
            try:
                result = subprocess.run(get_pods_cmd, shell=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

                # Check for "No resources found" in the output
                # terminating_pods = int(result.stdout.strip())
                # print(f"result {result}",flush=True)
                if "No resources found" in result.stderr:
                    print(f"No pods found in namespace {namespace}.", flush=True)
                    return True  # Pods are down
                else:
                    print(f"Pods still exist in namespace {namespace}. Waiting...", flush=True)

            except subprocess.CalledProcessError as e:
                print(f"Error checking for pods: {e.stderr}", flush=True)
                return False  # An error occurred

            time.sleep(1)  # Check every second

        print(f"Timeout waiting for pods to terminate in namespace {namespace}.", flush=True)
        return False  # Timeout reached

    def access_pod_and_initiate_gossip(self, pod_name, filename, unique_id, iteration):
        try:
            session = subprocess.Popen(['kubectl', 'exec', '-it', pod_name, '--request-timeout=600','--', 'sh'], stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if iteration == 0:
                message = f'{unique_id}-prepare'
            else:
                message = f'{filename[:-5]}-{unique_id}-{iteration}'
            session.stdin.write(f'python3 start.py --message {message}\n')
            session.stdin.flush()
            # end_time = time.time() + 300
            end_time = time.time() + 1000
            while time.time() < end_time:
                reads = [session.stdout.fileno()]
                ready = select.select(reads, [], [], 5)[0]
                if ready:
                    output = session.stdout.readline()
                    print(output, flush=True)
                    if 'Received acknowledgment:' in output:
                        if iteration > 0:
                            print("Gossip propagation complete.", flush=True)
                        break
                # Check if the session has ended
                if session.poll() is not None:
                    exit_code = session.poll()
                    print(f"Session ended (before completion) with exit code: {exit_code}", flush=True)
                    break
            else:
                print("Timeout waiting for gossip to complete.", flush=True)
                return False
            session.stdin.write('exit\n')
            session.stdin.flush()
            return True
        except Exception as e:
            print(f"Error accessing pod {pod_name}: {e}", flush=True)
            traceback.print_exc()
            return False


if __name__ == '__main__':

    # Getting the input or setting
    parser = argparse.ArgumentParser(description="Usage: python automate_all.py --num_test <number_of_tests>")
    parser.add_argument('--num_tests', required=True, type=int, help="Total number of tests to do")
    parser.add_argument('--num_nodes', required=True, type=int, help="Total number of nodes")
    args = parser.parse_args()

    test = Test(args.num_tests, args.num_nodes)  # Pass the new arguments to Test

    # helm name is fixed
    statefulsetname = 'gossip-statefulset'

    # Initiate helm chart and start the test based on nodes
    if test.filename:

        # if node == 10: We don't need to specify because nodes have been filtered
        if test.wait_for_pods_to_be_down(namespace='default', timeout=1000):

            # Apply helm
            result = test.run_command(['helm', 'install', statefulsetname, 'chartsim/', '--values',
                                       'chartsim/values.yaml', '--debug', '--set', 'testType=default',
                                       '--set','totalNodes=' + str(test.num_nodes)])

            print(f"Helm {statefulsetname}: {test.filename} started...", flush=True)

            if test.wait_for_pods_to_be_ready(namespace='default', expected_pods=test.num_nodes, timeout=1000):

                # Create unique uuid for this test
                unique_id = str(uuid.uuid4())[:4]  # Generate a UUID and take the first 4 characters

                # Test iteration starts here
                # for nt in range(1, test.num_tests + 1):
                for nt in range(test.num_tests + 1):

                    # Choosing gossip-statefulset-0 as initiator
                    # Can change this to random later
                    pod_name = "gossip-statefulset-0"

                    # Pods preparation phase, not counted as test
                    # first iteration (nt=0), for pods preparation
                    # next iteration (nt>0), for gossip test
                    if nt > 0:

                        print(f"Selected pod for test {nt}: {pod_name}", flush=True)
                        # Start accessing the pods and initiate gossip
                        if test.access_pod_and_initiate_gossip(pod_name, test.filename, unique_id, nt):
                            print(f"Test {nt} complete for {test.filename}.", flush=True)
                        else:
                            print(f"Test {nt} failed for {test.filename}.", flush=True)
                    else:
                        print(f"Pod preparation starting..", flush=True)
                        if test.access_pod_and_initiate_gossip(pod_name, test.filename, unique_id, nt):
                            print(f"Pod preparation complete for {test.filename}.", flush=True)
                        else:
                            print(f"Pod preparation failed for {test.filename}.", flush=True)
            else:
                print(f"Failed to prepare pods for {test.filename}.", flush=True)

            # Remove helm
            result = test.run_command(['helm', 'uninstall', statefulsetname])
            print(f"Helm {statefulsetname} will be uninstalled...", flush=True)
            if test.wait_for_pods_to_be_down(namespace='default', timeout=1000):
                print(f"Helm {statefulsetname}: {test.filename} uninstalled is completed...", flush=True)

    else:
        print(f"No file was found for args={args}")

