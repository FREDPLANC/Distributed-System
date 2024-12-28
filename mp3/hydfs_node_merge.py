import atexit
import hashlib
import subprocess
import time
from collections import defaultdict
import threading
from membership_node_old import Node, Introducer
from rw_lock import ReadWriteLock
from base36encode import AppendIdentifier
import os
import struct
import socket
import logging
import time
import base64
import shutil


logging.basicConfig(filename='local_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def getvmnum():
    hostname = socket.gethostname()
    return str(hostname.split('-')[-1].split('.')[0][1:])

class HyDFSNode:
    def __init__(self, node_id, membership_node: Node, storage_dir="hydfs_storage", cache_dir="hydfs_cache"):
        self.node_id = node_id
        self.membership_node = membership_node  

        self.cache = {}  
        # TO DO:

        self.cachelog = {}  # last operation of each file e.g {'file1': READ; 'file2': READ; 'file3': APPEND}

        # append encoder and decoder
        self.AppendIdentifier = AppendIdentifier()
        self.replication_factor = 5  

        
        self.file_store = set()  
        # file_store = {file_name_1, file_name_2, ...... }
        
        self.storage_dir = storage_dir  # Directory to store files on disk
        os.makedirs(self.storage_dir, exist_ok=True)  # Create storage directory if it doesn't exist
        self.cache_dir = cache_dir  # Directory to store files on disk
        os.makedirs(self.cache_dir, exist_ok=True)  # Create cache directory if it doesn't exist

        # lock
        self.rw_file_store_lock = ReadWriteLock() # lock both responsible and store
        self.cachelog_lock = threading.Lock()

        
        while not self.membership_node.get_membership_list():
            time.sleep(0.2)
        
        # thread task 2
        self.filePort = 10000
        threading.Thread(target=self.file_exchange_server, args=(self.filePort,), daemon=True).start()
        # self.file_exchange_server(self.filePort) # start another thread to receive file change task


        # thread task 1
        self.scan_interval = 3 
        threading.Thread(target=self.start_periodic_scan, daemon=True).start() 
        

        # thread task 3
        command_thread = threading.Thread(target=self.listen_for_commands, daemon=True).start()
        
        threading.Thread(target=self.listening_muti_append, args=(), daemon=True).start()

    def start_periodic_scan(self): 
        while True:
            try:
                self.check_and_update_responsibilities()
            except Exception as e:
                print(f"Error replicating: {e}")  
                
            time.sleep(self.scan_interval)
    
    def listen_for_commands(self):
        while True:
            try:
                command = input("Enter command: ").strip().split()
                if not command:
                    continue
                action = command[0].lower()
                if action == "create" and len(command) == 3:
                    localfilename = command[1]
                    HyDFSfilename = command[2]
                    self.create_file(localfilename, HyDFSfilename)
                elif action == "get" and len(command) == 3:
                    HyDFSfilename = command[1]
                    localfilename = command[2]
                    self.get_file(HyDFSfilename, localfilename)
                elif action == "append" and len(command) == 3:
                    localfilename = command[1]
                    HyDFSfilename = command[2]
                    self.append_file(localfilename, HyDFSfilename)
                elif action == "merge" and len(command) == 2:
                    HyDFSfilename = command[1]
                    self.merge_request_file(HyDFSfilename)
                elif action == "ls" and len(command) == 2:
                    HyDFSfilename = command[1]
                    self.print_hydfsfilename(HyDFSfilename)
                elif action == "store" and len(command) == 1:
                    self.handle_store()
                elif action == "getfromreplica" and len(command) == 4:
                    VMaddr = command[1]
                    HyDFSfilename = command[2]
                    Localfilename = command[3]
                    self.handle_get_replica(VMaddr, HyDFSfilename, Localfilename)
                elif action == "list_mem_ids" and len(command) == 1:
                    self.handle_list_mem()
                elif action == 'enable_sus':
                    self.membership_node.enable_sus()
                elif action == 'disable_sus':
                    self.membership_node.disable_sus()
                elif action == 'leave':
                    self.membership_node.leave_group()
                    break
                elif action == 'list_mem':
                    for node_id, data in self.membership_node.membership_list.items():
                        logging.info("status: %s, ID: %s", data['status'],node_id)
                elif action == 'list_self':
                    msg = "Self ID:" + self.membership_node.node_id
                    logging.debug(msg)
                elif action == 'grep':
                    continue
                elif action == 'status_sus':
                    if self.membership_node.detection_method == "PingAck+S":
                        print("PingAck+S is on")
                    else:
                        print("PingAck+S is off")
                elif action == 'list_file':
                    logging.info(self.global_file_list)
                elif action == 'mutiappend':
                    # if len(command) == 3:
                    #     times = int(command[1])
                    #     filename = command[2]
                    # #read input from file input
                    # else:
                    #     filename = 'input'
                    #     times = 1
                    # with open(filename, 'r') as f:
                    #         lines = f.readlines()
                    # lines = lines[0].split(' ')
                    # filename = lines[0]
                    # vmpair = []
                    # length = ((len(lines)-1)//2)
                    # for i in range(1, length+1):
                    #     vmpair.append((lines[i], lines[i+length]))
                    # for j in range(times):
                    #     self.send_mutiappend(filename, vmpair)
                    #     time.sleep(0.1)
                    with open('input', 'r') as f:
                        lines = f.readlines()
                    lines = lines[0].split(' ')
                    filename = lines[0]
                    vmpair = []
                    length = ((len(lines)-1)//2)
                    for i in range(1, length+1):
                        vmpair.append((lines[i], lines[i+length]))
                    self.send_mutiappend(filename, vmpair)


                else:
                    print("Invalid command. Use 'create localfilename HyDFSfilename' or 'get HyDFSfilename localfilename'.")



            except Exception as e:
                print(f"Error processing command: {e}")
    
    def send_mutiappend(self, filename, vmpair):
        for vm in vmpair:
            #send file name to each corresponding vm using muti thread 
            threading.Thread(target=self.send_file_to_vm, args=(vm, filename), daemon=False).start()

    def send_file_to_vm(self, vm, filename):
        # send file message to vm
        hostlist = [
            '172.22.157.76', '172.22.159.77', '172.22.95.76', 
            '172.22.157.77', '172.22.159.78', '172.22.95.77', 
            '172.22.157.78', '172.22.159.79', '172.22.95.78', 
            '172.22.157.79'
        ]
        ip = hostlist[int(vm[0][-1:])-1]
        try:
            with socket.create_connection((ip, 16888), timeout=10) as s:
                local = vm[1]
                remote = filename
                header = self.encode_mutiappend(local, remote)
                s.sendall(header)
                logging.info(f"Sent file {filename} to {ip}")
        except (socket.timeout, socket.error) as e:
            logging.error(f"Error sending file {filename} to {ip}: {e}")
    def encode_mutiappend(self, local, remote):
        return ('MUTI_APPD'+' '+remote + ' ' + local).encode()
    def decode_mutiappend(self, message):
        # decode the file and append to the local file
        message = message.decode()
        if message.split(' ')[0] != 'MUTI_APPD':
            return None
        return message.split(' ')[1], message.split(' ')[2]
    def listening_muti_append(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', 16888))
        server_socket.listen(10)
        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_muti_append, args=(client_socket, address), daemon=True).start()

    def handle_muti_append(self, client_socket, address):
        try:
            # rec all
            header = client_socket.recv(1024)
            
            if header is None:
                logging.info(f"Invalid header length from {address}")
                return
            remote, local= self.decode_mutiappend(header)
            self.append_file(local, remote)

        except Exception as e:
            logging.error(f"Error handling file request from {address}: {e}")
        finally:
            client_socket.close()




    def handle_list_mem(self):
        membership_list = self.membership_node.get_membership_list()
        hash_ring = {self._hash(node_id, prefix="node-"): node_id for node_id in membership_list.keys()}        
        sorted_hashes = sorted(hash_ring.keys())
        print("Membership list with IDs on the ring:")
        sorted_members = []
        for ring_id in sorted_hashes:
            node_id = hash_ring[ring_id]
            ip_address = membership_list[node_id]['ip']
            sorted_members.append((ring_id, node_id, ip_address))

        for ring_id, node_id, ip_address in sorted_members:
            print(f"Ring ID: {ring_id}, Node ID: {node_id}, IP Address: {ip_address}")

        return sorted_members
    
    
    def handle_get_replica(self, VMaddr, HyDFSfilename, Localfilename):
        responsible_nodes = self.get_responsible_nodes(HyDFSfilename)
        
        storage_path = os.path.join(self.storage_dir, HyDFSfilename)
        cache_path = os.path.join(self.cache_dir, HyDFSfilename)
        

        if self.node_id in responsible_nodes.keys() and responsible_nodes[self.node_id]['ip'] == VMaddr and os.path.exists(storage_path):
            logging.info(f"{HyDFSfilename} is available in local storage. Copying to cache.")

            if os.path.exists(cache_path):
                shutil.rmtree(cache_path) 
            shutil.copytree(storage_path, cache_path)
        else:
            for node_id, node_info in responsible_nodes.items():
                if node_info['ip'] == VMaddr:
    
                    op_signal = self.request_file_from_node(node_info, HyDFSfilename)
                    if op_signal is None:
                        logging.warning(f"Failed to retrieve {HyDFSfilename} from responsible nodes.")
                        return False
                    
                    if os.path.exists(cache_path):
                        shutil.rmtree(cache_path)  
                    shutil.copytree(storage_path, cache_path)
                    # because node is not responsible for that: delete it
                    shutil.rmtree(storage_path)
                    logging.info(f"Copied {HyDFSfilename} from storage to cache after retrieving from responsible nodes.")

        self.append_content_to_file(self.read_folder_content(cache_path), Localfilename)
    
    
    def handle_store(self):
        ring_id = self._hash(self.node_id, prefix="node-")
        print(f"Current VM ID on ring: {ring_id}")

        replicated_files = {}
        for folder_name in os.listdir(self.storage_dir):
            folder_path = os.path.join(self.storage_dir, folder_name)
            if os.path.isdir(folder_path):  
                file_hash = self._hash(folder_name, prefix="file-")
                replicated_files[folder_name] = file_hash

        print("Replicated files on HyDFS:")
        for filename, file_id in replicated_files.items():
            print(f"File Name: {filename}, File ID: {file_id}")

    
    def print_hydfsfilename(self, filename):
        responsible_nodes = self.get_responsible_nodes(filename)
        file_hash = self._hash(filename, prefix="file-")  

        print(f"File ID for {filename}: {file_hash}")
        print("Nodes responsible for storing this file:")

        for node_id, node_info in responsible_nodes.items():
            ring_id = self._hash(node_id, prefix="node-")  
            ip_address = node_info['ip']
            print(f"Ring ID: {ring_id}, Node ID: {node_id}, IP Address: {ip_address}")

    def merge_request_file(self, HyDFSfilename):
        timestart = time.time()
        global_file_list = self.membership_node.get_global_file_list()

        if HyDFSfilename in global_file_list:
            expected_responsible_nodes = self.get_responsible_nodes(HyDFSfilename)
            logging.info(f"Responsible Node for file {HyDFSfilename} is: {list(expected_responsible_nodes)}")
            if self.node_id in expected_responsible_nodes.keys():
                self.merge_request_file_from_responsible_nodes(expected_responsible_nodes, HyDFSfilename)
            else:
                for node_id, node_info in expected_responsible_nodes.items():
                    success = self.request_initiate_merge(node_info, HyDFSfilename)
                    if success:
                        logging.info(f"Requested node {node_id} to initiate merge for {HyDFSfilename}")
                        break  
                    else:
                        logging.info(f"Failed to request node {node_id} to initiate merge. Trying next.")
        else:
            logging.info(f"File {HyDFSfilename} not found in the global file list.")
        timeend = time.time()
        logging.info(f"Time taken for merge request: {timeend-timestart}")

    def merge_request_file_from_responsible_nodes(self, responsible_nodes, HyDFSfilename):
        success_count = 0
        for node_id, node_info in responsible_nodes.items():
            if node_id == self.node_id:
                continue
            
            if self.request_file_from_node(node_info, HyDFSfilename) is None:
                continue
            success_count += 1
        if success_count > self.replication_factor/2:
            timestamp = int(time.time() * 10) % 10**6
            self.merge_file(HyDFSfilename, timestamp=timestamp)
            
            # distribute to other nodes
            self.send_replace_file(responsible_nodes, HyDFSfilename, timestamp)
            print("Successful in merging operation")
        else:
            logging.info(f"Failed to retrieve enough file copies for {HyDFSfilename}.")
        return None
    
    def send_replace_file(self, responsible_nodes, HyDFSfilename, timestamp):
        file_dir = os.path.join(self.storage_dir, HyDFSfilename)
        original_file = None
        for filename in os.listdir(file_dir):
            if filename.startswith("original"):
                original_file = filename
                break

        if not original_file:
            logging.error(f"No original file found in directory {file_dir}")
        else:
            original_file_path = os.path.join(file_dir, original_file)

            for node_id, node_info in responsible_nodes.items():
                if node_id != self.node_id:
                    self.send_file_to_node(node_info, HyDFSfilename, original_file_path, timestamp)
                    logging.info(f"Sent original file {original_file} to node {node_id}")
    
    
    def merge_file(self, HyDFSfilename, timestamp):
        try:
            file_dir = os.path.join(self.storage_dir, HyDFSfilename)

            content = self.read_folder_content(file_dir)

            for filename in os.listdir(file_dir):
                file_path = os.path.join(file_dir, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    
            file_extension = os.path.splitext(HyDFSfilename)[1]
            new_filename = f"original_{timestamp}{file_extension}"
            new_file_path = os.path.join(file_dir, new_filename)

            with open(new_file_path, 'wb') as f:
                f.write(content)
                
            with self.rw_file_store_lock.acquire_write():
                self.file_store.add(new_filename)
            
        
        except Exception as e:
            logging.error(f"Error occurred during merge process for {HyDFSfilename}: {e}", exc_info=True)
            
    
    def request_initiate_merge(self, target_node, HyDFSfilename):
        target_ip = target_node['ip']
        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:

                header = self.create_header("MERGE_REQUEST", HyDFSfilename)
                s.sendall(header)
                logging.info(f"Sent merge request for {HyDFSfilename} to {target_ip}")
                return True
        except (socket.timeout, socket.error) as e:
            logging.error(f"Error sending merge request for {HyDFSfilename} to {target_ip}: {e}")
            return False
    
    
    
    def file_exchange_server(self, port=10000):
        """start listening server at port"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(10)

        # whenever a new connection is made, a new thread is created to handle the file request
        while True:
            client_socket, address = server_socket.accept() # blocking call
            threading.Thread(target=self.handle_file_request, args=(client_socket, address), daemon=True).start()

    def handle_file_request(self, client_socket, address):
        try:

            header = client_socket.recv(148) 
            if len(header) != 148:
                logging.info(f"Invalid header length from {address}")
                return

           # Decode header
            decoded_data = self.decode_header(header)
            if not decoded_data:
                logging.error(f"Failed to decode header from {address}")
                return
            
            operation_type, filename, timestamp, node_id, timevector = decoded_data
            logging.info(f"Received operation: {operation_type}, File: {filename}, "
                         f"Timestamp: {timestamp}, Node ID: {node_id} from {address}")
            
        
            if operation_type == "REQUEST_FILE":    
                file_dir = os.path.join(self.storage_dir, filename)
                if not os.path.exists(file_dir):
                    client_socket.sendall(b"ERR!")  
                    logging.warning(f"Directory {file_dir} not found for REQUEST_FILE operation")
                    return

                for file_name in os.listdir(file_dir):
                    file_path = os.path.join(file_dir, file_name)
                    if os.path.isfile(file_path):
                        file_type = "original" if "original" in file_name else "append"
                        file_size = os.path.getsize(file_path)

                        file_name_encoded = file_name.encode()
                        file_name_length = len(file_name_encoded)
                        header = struct.pack("!I", file_name_length) + file_name_encoded + struct.pack("!Q", file_size)

                        client_socket.sendall(header)

                        with open(file_path, 'rb') as f:
                            while chunk := f.read(4096):
                                client_socket.sendall(chunk)

                        logging.info(f"Sent {file_type} file {file_name} with size {file_size} to {address}")
                
                logging.info(f"All files in {file_dir} sent to {address} for REQUEST_FILE operation.")
                
            ###################################################
            
            elif operation_type == "SEND_FILE":
                file_dir = os.path.join(self.storage_dir, filename)

                if os.path.exists(file_dir):
                    shutil.rmtree(file_dir)
                
                os.makedirs(file_dir, exist_ok=True)

                
                file_path = os.path.join(file_dir, f"original_{timevector}{os.path.splitext(filename)[1]}")

                with open(file_path, 'wb') as f:
                    while chunk := client_socket.recv(4096):
                        f.write(chunk)
                
                with self.rw_file_store_lock.acquire_write():
                    self.file_store.add(filename)
                logging.info(f"Received file {filename} from {address}")

            elif operation_type == "APPEND_FILE":
                print(f"{time.time()}: receive append request")
                file_dir = os.path.join(self.storage_dir, filename)
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir, exist_ok=True)
                    logging.info(f"Append to filename{filename} with directory not exist.")
                    return

                file_extension = os.path.splitext(filename)[1]
                file_path = os.path.join(file_dir, f"append_{timestamp}_{node_id}{file_extension}")

                with open(file_path, 'wb') as f:
                    while chunk := client_socket.recv(4096):
                        f.write(chunk)
                logging.info(f"Appended to file {filename} at {file_path} from {address}")
                
                
                print(f"{time.time()}: Complete append request")

            elif operation_type == "MERGE_REQUEST":
                expected_responsible_nodes = self.get_responsible_nodes(filename)
                if self.node_id in expected_responsible_nodes.keys():
                    # self.merge_request_file_from_responsible_nodes(expected_responsible_nodes, filename)
                    threading.Thread(target=self.merge_request_file_from_responsible_nodes, args=(expected_responsible_nodes, filename,), daemon=False).start()
                    
            
            else:
                logging.info(f"Unknown operation type: {operation_type} from {address}")

        except Exception as e:
            logging.error(f"Error handling file request from {address}: {e}")
        finally:
            client_socket.close()

    # replica check func
    def check_and_update_responsibilities(self):
        global_file_list = self.membership_node.get_global_file_list()  
        for filename in global_file_list:
            expected_responsible_nodes = self.get_responsible_nodes(filename)  
            
            if self.node_id not in expected_responsible_nodes.keys():
                continue
            
            # logging.info(f"Node id {self.node_id}; responsible list {list(expected_responsible_nodes)}; filename: {filename}")
            

            with self.rw_file_store_lock.acquire_read():
                # logging.info(f"the file store is {list(self.file_store)}")
                if filename in self.file_store:
                    # logging.info(f"File {filename} already exists locally. Skipping replica request.")
                    continue
            logging.info(f"Replica Process: Missing File: {filename}")
            
            for node_id, node_info in expected_responsible_nodes.items():
                if node_id == self.node_id:
                    continue
                
                success = self.request_file_from_node(node_info, filename)
                if success:
                    # logging.info(f"Successfully retrieved {filename} from node {node_id}")
                    break  
                else:
                    logging.info(f"Node {node_id} does not have {filename}. Trying next node.")
 
    # request operation
    def request_file_from_responsible_nodes(self, responsible_nodes, HyDFSfilename):
        for node_id, node_info in responsible_nodes.items():
            if node_id == self.node_id:
                continue
            op_signal = self.request_file_from_node(node_info, HyDFSfilename)
            if op_signal is not None:
                return op_signal

        return None

    
    # receive file chunk (receive the whole directory)
    def request_file_from_node(self, target_node, HyDFSfilename):
        target_ip = target_node['ip']

        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
                header = self.create_header("REQUEST_FILE", HyDFSfilename)
                s.sendall(header)
                local_dir = os.path.join(self.storage_dir, HyDFSfilename)
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                    # logging.info(f"Created directory {local_dir} for storing received files")

                while True:
                    header_data = s.recv(4)
                    
                    if not header_data:
                        # logging.info(f"Replica for file {HyDFSfilename} is done")
                        break  
                    
                    if header_data == b"ERR!":
                        logging.error(f"Received error message from {target_ip}: Directory {HyDFSfilename} not found.")
                        return None  
                    
                    filename_length = struct.unpack("!I", header_data)[0]

                    # filename = s.recv(filename_length).decode()
                    try:
                        filename = s.recv(filename_length).decode('utf-8')
                    except UnicodeDecodeError as e:
                        logging.error(f"Failed to decode filename. Data: {s.recv(filename_length)}, Error: {e}")
                        return None

                    file_size_data = s.recv(8)
                    file_size = struct.unpack("!Q", file_size_data)[0]

                    # logging.info(f"{HyDFSfilename}: Receiving file chunk: {filename} ({file_size} bytes)")

                    received_data = bytearray()
                    while len(received_data) < file_size:
                        chunk = s.recv(min(4096, file_size - len(received_data)))
                        if not chunk:
                            break
                        received_data.extend(chunk)
                    
                    full_path = os.path.join(local_dir, filename)

                    if os.path.exists(full_path):
                        logging.info(f"File {filename} already exists in {local_dir}. Skipping storage.")
                    else:
                        if len(received_data) == file_size:
                            # logging.info(f"Successfully received file: {filename} ({file_size} bytes)")
                            with open(full_path, 'wb') as f:
                                f.write(received_data)
                            with self.rw_file_store_lock.acquire_write():
                                self.file_store.add(HyDFSfilename)
                            # logging.info(f"Added {full_path} to file_store")
                        else:
                            logging.warning(f"Failed to receive complete file: {filename} from {target_ip}")
                            
                return 1


        except (socket.timeout, socket.error) as e:
            logging.error(f"Error requesting file {HyDFSfilename} from {target_ip}: {e}")
            return None
        
    def append_file(self, localfilename, HyDFSfilename):
        try:
            with open(localfilename, 'rb') as f:
                file_content = f.read()

            self.update_cachelog(HyDFSfilename, "APPEND")
            header = self.create_header("APPEND_FILE", HyDFSfilename)
            responsible_nodes = self.get_responsible_nodes(HyDFSfilename)
            logging.info(f"Responsible Node for file {HyDFSfilename} is: {list(responsible_nodes)}")
            
        
            for node_id, node_info in responsible_nodes.items():
                if node_id == self.node_id:
                    self.append_to_local_file(HyDFSfilename, header, file_content)
                    # logging.info(f"Appended {localfilename} to local copy of {HyDFSfilename}")
                else:
                    self.send_append_to_node(node_info, HyDFSfilename,header + file_content)
            # logging.info(f"Appended {localfilename} to {HyDFSfilename} on distributed nodes.")

            print("send append operation complete")
        except Exception as e:
            logging.error(f"Failed to append file {localfilename} to {HyDFSfilename}: {e}")

    def send_append_to_node(self, target_node, HyDFSfilename, file_data):
        target_ip = target_node['ip']
        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
                s.sendall(file_data)
                # logging.info(f"Appended content to {HyDFSfilename} on node {target_ip}")
        except (socket.timeout, socket.error) as e:
            logging.error(f"Error appending file {HyDFSfilename} to {target_ip}: {e}")

    def append_to_local_file(self, HyDFSfilename, header, content):
        file_dir = os.path.join(self.storage_dir, HyDFSfilename)
        if not os.path.exists(file_dir):
            logging.error(f"Local directory for {HyDFSfilename} does not exist.")
            return
        _, _, timestamp, node_id, timevector = self.decode_header(header)

        file_extension = os.path.splitext(HyDFSfilename)[1]
        file_path = os.path.join(file_dir, f"append_{timestamp}_{node_id}{file_extension}")

        with open(file_path, 'wb') as f:
            f.write(content)
            
        
        # logging.info(f"Appended content to local file {HyDFSfilename} at {file_path}")
        


    def create_header(self, operation_type, filename, time_vector_number=0):



        operation_type_field = operation_type.ljust(20)[:20]  
        filename_field = filename.ljust(100)[:100]            
        time_vector_field = time_vector_number


        timestamp = int(time.time() * 10) % 10**6

        placeholder = f"{timestamp}_{self.node_id}".ljust(20)[:20] 
        
        header = struct.pack(f'20s 100s Q 20s', 
                             operation_type_field.encode(), 
                             filename_field.encode(), 
                             time_vector_field, 
                             placeholder.encode())
        return header
    
    def decode_header(self, header):
 

        operation_type_field, filename_field, time_vector_field, placeholder_field = struct.unpack(f'20s 100s Q 20s', header)


        operation_type = operation_type_field.decode().strip()
        filename = filename_field.decode().strip()
        timevector = time_vector_field


        placeholder = placeholder_field.decode().strip()
        try:
            timestamp_str, node_id_str = placeholder.split("_")
            timestamp = int(timestamp_str)
            node_id = node_id_str  
        except ValueError:
            logging.error("Invalid placeholder format.")
            return None

        return operation_type, filename, timestamp, node_id, timevector

    def get_file(self, HyDFSfilename, localfilename):
        flag = 0
        logging.info(f"cachelog : {self.cachelog}")
        with self.cachelog_lock:
            if HyDFSfilename in self.cachelog and self.cachelog[HyDFSfilename] == "READ":
                flag = 1
        

        cache_path = os.path.join(self.cache_dir, HyDFSfilename)
        storage_path = os.path.join(self.storage_dir, HyDFSfilename)
        
        
        if flag == 1 and os.path.exists(cache_path):
            logging.info(f"{HyDFSfilename} found in cache. Skipping request to other nodes.")
        else:

            responsible_nodes = self.get_responsible_nodes(HyDFSfilename)

            logging.info(f"No cache! Responsible Node for file {HyDFSfilename} is: {list(responsible_nodes)}")
            

            if self.node_id in responsible_nodes.keys() and os.path.exists(storage_path):
                logging.info(f"{HyDFSfilename} is available in local storage. Copying to cache.")

                if os.path.exists(cache_path):
                    shutil.rmtree(cache_path)  
                shutil.copytree(storage_path, cache_path)
            else:
                op_signal = self.request_file_from_responsible_nodes(responsible_nodes, HyDFSfilename)
                if op_signal is None:
                    logging.warning(f"Failed to retrieve {HyDFSfilename} from responsible nodes.")
                    return False
                if os.path.exists(cache_path):
                    shutil.rmtree(cache_path)  
                shutil.copytree(storage_path, cache_path)
                
                # because node is not responsible for that: delete it
                shutil.rmtree(storage_path)
                logging.info(f"Copied {HyDFSfilename} from storage to cache after retrieving from responsible nodes.")

    
        self.append_content_to_file(self.read_folder_content(cache_path), localfilename)
        self.update_cachelog(HyDFSfilename, "READ")
        
        print("get operation complete")
        return True

    def read_folder_content(self, folder_path):
        files = os.listdir(folder_path)
        def sort_key(filename):
            if filename == "original" or filename.startswith("original"):
                return (0, "", 0)
            parts = filename.split('_')
            if len(parts) >= 3 and parts[0] == "append":
                timestamp = int(parts[1])  
                node_id = parts[2].split('.')[0]  
                return (1, node_id, timestamp)
            else:
                return (2, filename, 0)  

        files_sorted = sorted(files, key=sort_key)
        combined_content = bytearray()
        for filename in files_sorted:
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                with open(file_path, 'rb') as f:
                    while chunk := f.read(4096):
                        combined_content.extend(chunk)
                logging.info(f"Read content of {filename} from {folder_path}")

        return bytes(combined_content)

    def append_content_to_file(self, content, target_file):
        if not os.path.exists(target_file):
            open(target_file, 'wb').close()

        with open(target_file, 'ab') as f:
            f.write(content)
        logging.info(f"Appended content to {target_file}")

    def update_cachelog(self, filename, operation):
        with self.cachelog_lock:
            self.cachelog[filename] = operation
       

    def create_file(self, localfilename, HyDFSfilename):
        """Creates a file and distributes it to the responsible nodes."""

        if HyDFSfilename in self.membership_node.get_global_file_list():
            print(f"File {HyDFSfilename} already exists in the distributed system. Creation failed.")
            return False
        if not os.path.exists(localfilename):
            print(f"Local file {localfilename} does not exist.")
            return False
        responsible_nodes = self.get_responsible_nodes(HyDFSfilename)
        logging.info(f"Responsible Node for file {HyDFSfilename} is: {list(responsible_nodes)}")
        
        # logging.info(f"list is {list(self.membership_node.get_global_file_list())}")
        # Distribute the file to responsible nodes
        self.distribute_file(HyDFSfilename, localfilename, responsible_nodes)
        
        self.membership_node.update_global_file_list(HyDFSfilename)
        
        print("send create request complete")
        return True


    def distribute_file(self, HyDFSfilename, localfilename, responsible_nodes):
        """Distributes the file to its responsible nodes, excluding the current node if applicable."""
        
        timestamp = int(time.time() * 10) % 10**6

        for node_id, node_info in responsible_nodes.items():
            if node_id != self.node_id:
                self.send_file_to_node(node_info, HyDFSfilename, localfilename, timestamp)
            else:
                self.send_file_local(HyDFSfilename, localfilename, timestamp)

    def send_file_to_node(self, target_node, HyDFSfilename, file_path, time=0):

        target_ip = target_node['ip']
        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:

                header = self.create_header("SEND_FILE", HyDFSfilename, time_vector_number=time)
                s.sendall(header)


                with open(file_path, 'rb') as f:
                    while chunk := f.read(4096):  
                        s.sendall(chunk)

        except (socket.timeout, socket.error) as e:
            print(f"Error sending file {HyDFSfilename} to {target_ip}: {e}")

    def send_file_local(self, HyDFSfilename, localfilename, timestamp):

        file_dir = os.path.join(self.storage_dir, HyDFSfilename)
        os.makedirs(file_dir, exist_ok=True)
        original_path = os.path.join(file_dir, f"original_{timestamp}{os.path.splitext(HyDFSfilename)[1]}")


        with open(localfilename, 'rb') as src_file:
            with open(original_path, 'wb') as dest_file:
                dest_file.write(src_file.read())


        with self.rw_file_store_lock.acquire_write():
            self.file_store.add(HyDFSfilename)

    
        
    def _hash(self, key, prefix=""):
        return int(hashlib.sha1((prefix + key).encode()).hexdigest(), 16)

    def get_responsible_nodes(self, filename):
        file_hash = self._hash(filename, prefix="file-")  
        membership_list = self.membership_node.get_membership_list()  
        hash_ring = {self._hash(node_id, prefix="node-"): node_id for node_id in membership_list.keys()}
        sorted_hashes = sorted(hash_ring.keys())

        primary_index = 0
        for i in range(len(sorted_hashes)):
            if sorted_hashes[i] >= file_hash:
                primary_index = i
                break
        else:
            primary_index = 0  

        responsible_nodes = {}
        for i in range(self.replication_factor):
            responsible_index = (primary_index + i) % len(sorted_hashes)
            responsible_node_id = hash_ring[sorted_hashes[responsible_index]]
            responsible_nodes[responsible_node_id] = membership_list[responsible_node_id]
        return responsible_nodes



def delete_folder(folder_name):

    if os.path.isdir(folder_name):
        shutil.rmtree(folder_name)
        print(f"{folder_name} deleted.")
    else:
        print(f"{folder_name} not exisit.")

def clear_file(file_name):
    if os.path.isfile(file_name):
        with open(file_name, 'w'):
            pass 
        print(f"{file_name} clear.")
    else:
        print(f"{file_name} not exsist.")

def clean_folder():
    delete_folder("hydfs_storage")
    delete_folder("hydfs_cache")

    clear_file("local_log.log")
    clear_file('bandwidth_usage.log')

def encodeid(ip, port, incarnation, node_id):
    return f"vm{getvmnum()[-1]}#{ip[-2:]}{str(incarnation)[-3:]}"

if __name__ == "__main__":
    # if getvmnum() == '001':
    clean_folder()
    monitor_process = subprocess.Popen(["python3", "/home/twei11/ga0/mp3/monitor.py"])
    def cleanup():
        if monitor_process.poll() is None:
            monitor_process.terminate() 
            monitor_process.wait()  
    atexit.register(cleanup)      
    
    nodeId = int(getvmnum())
    candidate_list = ['172.22.157.76', '172.22.159.77', '172.22.95.76']  
    vm1ip = '172.22.157.76'
    vm1port = '9999'
    hostlist = [
            '172.22.157.76', '172.22.159.77', '172.22.95.76', 
            '172.22.157.77', '172.22.159.78', '172.22.95.77', 
            '172.22.157.78', '172.22.159.79', '172.22.95.78', 
            '172.22.157.79'
        ]
    
    ip = hostlist[int(getvmnum()[-1:])-1]
    port = '9999'
    incarnation = int(time.time() * 1000)
    node_id = f"{ip}:{port}#{incarnation}"
    
    newNodeId = encodeid(ip, port, incarnation, node_id)
    # print(getvmnum())
    if getvmnum() == '001':
        membership_node = Introducer(vm1ip, vm1port, 'PingAck', 0, incarnation=incarnation)
    else:
        membership_node = Node(hostlist[int(getvmnum()[-1:])-1],'9999', vm1ip+":"+vm1port, 'PingAck', 0, incarnation=incarnation)
    # membership_node = MembershipNode(node_id=candidate_list[nodeId-1])
    time.sleep(1)
    hydfs_node = HyDFSNode(node_id=newNodeId, membership_node=membership_node)
    while True:
        time.sleep(10)

    
