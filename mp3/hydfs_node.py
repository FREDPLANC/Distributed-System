import hashlib
import time
from collections import defaultdict
import threading
from membership_node import MembershipNode  # 引入已有的 membership_node 类
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
        """
        初始化 HyDFS 节点。
        :param node_id: 当前节点的 ID
        :param membership_node: 一个 MembershipNode 对象，用于访问节点的 membership 列表和故障检测功能
        """
        self.node_id = node_id
        self.membership_node = membership_node  # 依赖注入的方式引用 membership_node

        self.cache = {}  # 缓存已读文件
        # TO DO:

        self.cachelog = {}  # last operation of each file e.g {'file1': READ; 'file2': READ; 'file3': APPEND}

        # append encoder and decoder
        self.AppendIdentifier = AppendIdentifier()
        self.replication_factor = 2  # 文件复制因子，允许最多两个节点故障

        
        self.file_store = set()  # 本地存储文件
        # file_store = {file_name_1, file_name_2, ...... }
        
        self.storage_dir = storage_dir  # Directory to store files on disk
        os.makedirs(self.storage_dir, exist_ok=True)  # Create storage directory if it doesn't exist
        self.cache_dir = cache_dir  # Directory to store files on disk
        os.makedirs(self.cache_dir, exist_ok=True)  # Create cache directory if it doesn't exist

        # lock
        self.rw_file_store_lock = ReadWriteLock() # lock both responsible and store
        self.cachelog_lock = threading.Lock()

        # thread task 2
        self.filePort = 10000
        threading.Thread(target=self.file_exchange_server, args=(self.filePort,), daemon=True).start()
        # self.file_exchange_server(self.filePort) # start another thread to receive file change task


        # thread task 1
        self.scan_interval = 3  # 定期检查的时间间隔（秒）
        threading.Thread(target=self.start_periodic_scan, daemon=True).start()  # 启动扫描线程
        

        # thread task 3
        command_thread = threading.Thread(target=self.listen_for_commands, daemon=True).start()
        
    def start_periodic_scan(self): 
        while True:
            try:
                self.check_and_update_responsibilities()
            except Exception as e:
                print(f"Error replicating: {e}")  
                
            time.sleep(self.scan_interval)
    
    def listen_for_commands(self):
        """监听终端输入并处理命令"""
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
                    self.merge_file(HyDFSfilename)
                else:
                    print("Invalid command. Use 'create localfilename HyDFSfilename' or 'get HyDFSfilename localfilename'.")
            except Exception as e:
                print(f"Error processing command: {e}")
    
    def merge_file(self, HyDFSfilename):
        # 获取最新的全局文件列表
        global_file_list = self.membership_node.get_global_file_list()

        # 检查文件是否存在
        if HyDFSfilename in global_file_list:
            # 获取负责该文件的节点列表
            expected_responsible_nodes = self.get_responsible_nodes(HyDFSfilename)

            # 检查当前节点是否是负责节点之一
            if self.node_id in expected_responsible_nodes.keys():
                # 当前节点是负责节点之一，直接发送合并请求
                self.request_file_from_responsible_nodes(self, expected_responsible_nodes, HyDFSfilename)
            else:
                # 当前节点不是负责节点，尝试向每个负责节点发起合并请求，直到成功
                for node_id, node_info in expected_responsible_nodes.items():
                    # 尝试请求此节点发起合并
                    success = self.request_initiate_merge(node_info, HyDFSfilename)
                    if success:
                        logging.info(f"Requested node {node_id} to initiate merge for {HyDFSfilename}")
                        break  # 请求成功，退出循环
                    else:
                        logging.info(f"Failed to request node {node_id} to initiate merge. Trying next.")
        else:
            # 文件不存在，记录日志
            logging.info(f"File {HyDFSfilename} not found in the global file list.")
    
    
    def request_initiate_merge(self, target_node, HyDFSfilename):
        """向目标节点发送合并请求"""
        target_ip = target_node['ip']
        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
                # 创建并发送包含操作类型的报头
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
        server_socket.listen(5) # 最大连接数为 5

        # whenever a new connection is made, a new thread is created to handle the file request
        while True:
            client_socket, address = server_socket.accept() # blocking call
            threading.Thread(target=self.handle_file_request, args=(client_socket, address), daemon=True).start()

    def handle_file_request(self, client_socket, address):
        """处理来自其他节点的文件请求和接收"""
        try:
            # 先接收定长报头
            header = client_socket.recv(148)  # 148 字节的报头长度
            if len(header) != 148:
                logging.info(f"Invalid header length from {address}")
                return

           # Decode header
            decoded_data = self.decode_header(header)
            if not decoded_data:
                logging.error(f"Failed to decode header from {address}")
                return
            
            operation_type, filename, timestamp, node_id = decoded_data
            logging.info(f"Received operation: {operation_type}, File: {filename}, "
                         f"Timestamp: {timestamp}, Node ID: {node_id} from {address}")
            
        
            if operation_type == "REQUEST_FILE":
                # 构建文件路径
                # file_path = os.path.join(self.storage_dir, filename, "original" + os.path.splitext(filename)[1])
                # if os.path.exists(file_path):
                #     with open(file_path, 'rb') as f:
                #         while chunk := f.read(4096):
                #             client_socket.sendall(chunk)
                #     logging.info(f"Sent file {filename} to {address}")
                # else:
                #     client_socket.sendall(b"ERROR: File not found")
                    
                ###################################################
                
                # 获取文件夹路径
                file_dir = os.path.join(self.storage_dir, filename)
                if not os.path.exists(file_dir):
                    client_socket.sendall(b"ERR!")  # 错误标识符
                    logging.warning(f"Directory {file_dir} not found for REQUEST_FILE operation")
                    return

                # 遍历文件夹中的所有文件
                for file_name in os.listdir(file_dir):
                    file_path = os.path.join(file_dir, file_name)
                    if os.path.isfile(file_path):
                        file_type = "original" if "original" in file_name else "append"
                        file_size = os.path.getsize(file_path)

                        # 创建文件头部：包含文件类型、文件名长度、文件名和文件大小
                        file_name_encoded = file_name.encode()
                        file_name_length = len(file_name_encoded)
                        header = struct.pack("!I", file_name_length) + file_name_encoded + struct.pack("!Q", file_size)

                        # 发送头部
                        client_socket.sendall(header)

                        # 发送文件内容
                        with open(file_path, 'rb') as f:
                            while chunk := f.read(4096):
                                client_socket.sendall(chunk)

                        logging.info(f"Sent {file_type} file {file_name} with size {file_size} to {address}")
                
                logging.info(f"All files in {file_dir} sent to {address} for REQUEST_FILE operation.")
                
            ###################################################
            
            elif operation_type == "SEND_FILE":
                # 接收并存储文件
                file_dir = os.path.join(self.storage_dir, filename)
                os.makedirs(file_dir, exist_ok=True)
                file_path = os.path.join(file_dir, "original" + os.path.splitext(filename)[1])

                with open(file_path, 'wb') as f:
                    while chunk := client_socket.recv(4096):
                        f.write(chunk)
                
                with self.rw_file_store_lock.acquire_write():
                    self.file_store.add(filename)
                logging.info(f"Received file {filename} from {address}")

            elif operation_type == "APPEND_FILE":
                # 检查目标文件夹是否存在
                file_dir = os.path.join(self.storage_dir, filename)
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir, exist_ok=True)
                    logging.info(f"Append to filename{filename} with directory not exist.")
                    return

                file_extension = os.path.splitext(filename)[1]
                file_path = os.path.join(file_dir, f"append_{timestamp}_{node_id}{file_extension}")

                # 接收追加的数据并存储
                with open(file_path, 'wb') as f:
                    while chunk := client_socket.recv(4096):
                        f.write(chunk)
                logging.info(f"Appended to file {filename} at {file_path} from {address}")

            else:
                logging.info(f"Unknown operation type: {operation_type} from {address}")

        except Exception as e:
            logging.error(f"Error handling file request from {address}: {e}")
        finally:
            client_socket.close()

    # replica check func
    def check_and_update_responsibilities(self):
        global_file_list = self.membership_node.get_global_file_list()  # 获取最新的全局文件列表

        for filename in global_file_list:
            expected_responsible_nodes = self.get_responsible_nodes(filename)  # 获取负责的节点列表
            
            if self.node_id not in expected_responsible_nodes.keys():
                continue
            
            logging.info(f"Node id {self.node_id}; responsible list {list(expected_responsible_nodes)}; filename: {filename}")
            
            # 如果当前节点已经有文件，则跳过该文件
            with self.rw_file_store_lock.acquire_read():
                logging.info(f"the file store is {list(self.file_store)}")
                if filename in self.file_store:
                    logging.info(f"File {filename} already exists locally. Skipping replica request.")
                    continue
            logging.info(f"Replica Process: Missing File: {filename}")
            
            # 挨个向每个节点请求文件副本
            for node_id, node_info in expected_responsible_nodes.items():
                # 跳过自身节点，避免请求自己
                if node_id == self.node_id:
                    continue
                
                # 请求文件副本
                success = self.request_file_from_node(node_info, filename)
                if success:
                    logging.info(f"Successfully retrieved {filename} from node {node_id}")
                    break  # 成功获取文件后停止请求
                else:
                    logging.info(f"Node {node_id} does not have {filename}. Trying next node.")
 
    # request operation
    def request_file_from_responsible_nodes(self, responsible_nodes, HyDFSfilename):
        
        """
        向责任节点列表中的节点逐一发送文件请求，直到成功接收文件内容。
        :param responsible_nodes: 字典格式，键是 node_id，值是节点信息（如 {'ip': ..., 'incredible': ...}）
        :param HyDFSfilename: 需要请求的文件名
        """
        for node_id, node_info in responsible_nodes.items():
            # 如果当前节点是请求列表中的节点，跳过自己
            if node_id == self.node_id:
                continue

            op_signal = self.request_file_from_node(node_info, HyDFSfilename)
            
            # (暂时）如果成功接收文件内容，返回该内容
            if op_signal is not None:
                return op_signal

        return None

    
    # receive file chunk (receive the whole directory)
    def request_file_from_node(self, target_node, HyDFSfilename):
        """向目标节点发送文件请求并接收文件夹中的所有文件内容"""
        target_ip = target_node['ip']

        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
                # 创建并发送包含操作类型的报头
                header = self.create_header("REQUEST_FILE", HyDFSfilename)
                s.sendall(header)

                # 开始接收文件夹中的所有文件
                # 创建本地文件夹（如果不存在）
                local_dir = os.path.join(self.storage_dir, HyDFSfilename)
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                    logging.info(f"Created directory {local_dir} for storing received files")

                while True:
                    # 接收文件头部信息：文件名长度（4字节）
                    header_data = s.recv(4)
                    
                    if not header_data:
                        logging.info(f"Replica for file {HyDFSfilename} is done")
                        break  # 连接关闭或所有文件接收完成
                    
                    if header_data == b"ERR!":
                        logging.error(f"Received error message from {target_ip}: Directory {HyDFSfilename} not found.")
                        return None  # 提前停止并返回 None
                    
                    # 解析文件名长度
                    filename_length = struct.unpack("!I", header_data)[0]

                    # # 接收文件名
                    # filename = s.recv(filename_length).decode()
                    try:
                        filename = s.recv(filename_length).decode('utf-8')
                    except UnicodeDecodeError as e:
                        logging.error(f"Failed to decode filename. Data: {s.recv(filename_length)}, Error: {e}")
                        # 这里可以根据需要处理错误，例如返回 None 或退出函数
                        return None

                    # 接收文件大小（8字节）
                    file_size_data = s.recv(8)
                    file_size = struct.unpack("!Q", file_size_data)[0]

                    logging.info(f"{HyDFSfilename}: Receiving file chunk: {filename} ({file_size} bytes)")

                    # 接收文件内容
                    received_data = bytearray()
                    while len(received_data) < file_size:
                        chunk = s.recv(min(4096, file_size - len(received_data)))
                        if not chunk:
                            break
                        received_data.extend(chunk)
                    
                    # 构建文件的完整路径
                    full_path = os.path.join(local_dir, filename)

                    if os.path.exists(full_path):
                        logging.info(f"File {filename} already exists in {local_dir}. Skipping storage.")
                    else:
                        # 检查是否成功接收到完整文件
                        if len(received_data) == file_size:
                            logging.info(f"Successfully received file: {filename} ({file_size} bytes)")
                            # 将文件内容存储到文件系统
                            with open(full_path, 'wb') as f:
                                f.write(received_data)
                            with self.rw_file_store_lock.acquire_write():
                                self.file_store.add(HyDFSfilename)
                            logging.info(f"Added {full_path} to file_store")
                        else:
                            logging.warning(f"Failed to receive complete file: {filename} from {target_ip}")


        except (socket.timeout, socket.error) as e:
            logging.error(f"Error requesting file {HyDFSfilename} from {target_ip}: {e}")
            return None
    
    # def request_file_from_node(self, target_node, HyDFSfilename):
    #     """向目标节点发送文件请求并接收文件内容"""
    #     target_ip = target_node['ip']

    #     try:
    #         with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
    #             # 创建并发送包含操作类型的报头
    #             header = self.create_header("REQUEST_FILE", HyDFSfilename)
    #             s.sendall(header)

    #             # 接收响应的文件内容
    #             file_content = bytearray()
    #             while True:
    #                 chunk = s.recv(4096)  # 每次接收 4KB 数据
    #                 if not chunk:
    #                     break
    #                 file_content.extend(chunk)

    #             if file_content:
    #                 logging.info(f"Successfully received {HyDFSfilename} from {target_ip}")
    #                 with self.rw_file_store_lock.acquire_write():
    #                     self.file_store.add(HyDFSfilename)
    #                 logging.info(f"add {HyDFSfilename} to file_store ")
    #                 return file_content

                

    #     except (socket.timeout, socket.error) as e:
    #         print(f"Error requesting file {HyDFSfilename} from {target_ip}: {e}")
    #         return None
        
    
    def append_file(self, localfilename, HyDFSfilename):
        """将本地文件内容追加到分布式文件系统中的指定文件"""
        try:
            with open(localfilename, 'rb') as f:
                file_content = f.read()

            # 创建 APPEND_FILE 请求头
            header = self.create_header("APPEND_FILE", HyDFSfilename)
            responsible_nodes = self.get_responsible_nodes(HyDFSfilename)

            # 遍历负责节点列表，检查是否包含当前节点
            for node_id, node_info in responsible_nodes.items():
                if node_id == self.node_id:
                    # 如果当前节点是负责节点之一，直接在本地进行追加操作
                    self.append_to_local_file(HyDFSfilename, header, file_content)
                    logging.info(f"Appended {localfilename} to local copy of {HyDFSfilename}")
                else:
                    # 否则，发送 append 请求到其他负责节点
                    self.send_append_to_node(node_info, HyDFSfilename,header + file_content)
            logging.info(f"Appended {localfilename} to {HyDFSfilename} on distributed nodes.")

        except Exception as e:
            logging.error(f"Failed to append file {localfilename} to {HyDFSfilename}: {e}")

    def send_append_to_node(self, target_node, HyDFSfilename, file_data):
        """发送 append 请求到目标节点"""
        target_ip = target_node['ip']
        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
                s.sendall(file_data)
                logging.info(f"Appended content to {HyDFSfilename} on node {target_ip}")
        except (socket.timeout, socket.error) as e:
            logging.error(f"Error appending file {HyDFSfilename} to {target_ip}: {e}")

    def append_to_local_file(self, HyDFSfilename, header, content):
        """直接将内容追加到本地的文件副本"""
        file_dir = os.path.join(self.storage_dir, HyDFSfilename)
        if not os.path.exists(file_dir):
            logging.error(f"Local directory for {HyDFSfilename} does not exist.")
            return
        # 从 header 提取 timestamp 和 node_id
        _, _, timestamp, node_id = self.decode_header(header)

        # 使用统一的文件名格式追加内容
        file_extension = os.path.splitext(HyDFSfilename)[1]
        file_path = os.path.join(file_dir, f"append_{timestamp}_{node_id}{file_extension}")

        with open(file_path, 'wb') as f:
            f.write(content)
        logging.info(f"Appended content to local file {HyDFSfilename} at {file_path}")
        

    # untested: assumption : 假设nodeid是整数
    def create_header(self, operation_type, filename, time_vector_number=0):
        """
        创建包含操作类型、文件名、时间戳和 node_id 的报头，时间戳不进行压缩。
        """
        operation_type_field = operation_type.ljust(20)[:20]  # 数据类型字段
        filename_field = filename.ljust(100)[:100]            # 文件名字段
        time_vector_field = time_vector_number
        # 使用8字节浮点数表示 timestamp，4字节整数表示 node_id
        # 生成简化的时间戳，仅保留时间戳的最后 6 位
        timestamp = int(time.time() * 10) % 10**6

        # 使用完整的 node_id（假设最大长度为 20 字节）
        placeholder = f"{timestamp}_{self.node_id}".ljust(20)[:20]  # 20 字节
        
        # 使用 struct 打包定长报头
        header = struct.pack(f'20s 100s Q 20s', 
                             operation_type_field.encode(), 
                             filename_field.encode(), 
                             time_vector_field, 
                             placeholder.encode())
        logging.info(placeholder)
        return header
    
    # def create_header(self, operation_type, filename, time_vector_number=0):
    #     # 确保数据类型和文件名字段固定长度
    #     operation_type_field = operation_type.ljust(20)[:20]  # 数据类型字段，20 字节
    #     filename_field = filename.ljust(100)[:100]  # 文件名字段，100 字节
    #     time_vector_field = time_vector_number  # 8 字节整数

    #     # 创建 20 字节的留位符字段
    #     placeholder_field = ' ' * 20  # 留位符字段

    #     # 使用 struct 打包定长报头（148 字节）
    #     header = struct.pack(f'20s 100s Q 20s', operation_type_field.encode(), filename_field.encode(), time_vector_field, placeholder_field.encode())
    #     return header
    def decode_header(self, header):
        """
        Decode a 148-byte header to extract operation_type, filename, timestamp, and node_id.
        """
        # Unpack fields from the header
        operation_type_field, filename_field, time_vector_field, placeholder_field = struct.unpack(f'20s 100s Q 20s', header)

        # Decode and strip padding from the fields
        operation_type = operation_type_field.decode().strip()
        filename = filename_field.decode().strip()
        timestamp = time_vector_field

        # Extract timestamp and node_id from the placeholder
        placeholder = placeholder_field.decode().strip()
        logging.info(placeholder)
        try:
            timestamp_str, node_id_str = placeholder.split("_")
            timestamp = int(timestamp_str)
            node_id = node_id_str  # node_id 保持为字符串形式
        except ValueError:
            logging.error("Invalid placeholder format.")
            return None

        return operation_type, filename, timestamp, node_id

    def get_file(self, HyDFSfilename, localfilename):
        flag = 0
        logging.info(f"cachelog : {self.cachelog}")
        with self.cachelog_lock:
            if HyDFSfilename in self.cachelog and self.cachelog[HyDFSfilename] == "READ":
                flag = 1
        
        # 根据 cachelog 判断是否可以直接从缓存读取
        cache_path = os.path.join(self.cache_dir, HyDFSfilename)
        storage_path = os.path.join(self.storage_dir, HyDFSfilename)
        
        
        if flag == 1 and os.path.exists(cache_path):
            logging.info(f"{HyDFSfilename} found in cache. Skipping request to other nodes.")
        else:
            # 获取负责节点列表
            responsible_nodes = self.get_responsible_nodes(HyDFSfilename)

            # 如果当前节点是责任节点，直接读取文件内容
            if self.node_id in responsible_nodes.keys() and os.path.exists(storage_path):
                logging.info(f"{HyDFSfilename} is available in local storage. Copying to cache.")
                # 将存储中的文件夹复制到缓存目录
                if os.path.exists(cache_path):
                    shutil.rmtree(cache_path)  # 如果缓存中已存在该文件夹，先删除
                shutil.copytree(storage_path, cache_path)
            else:
                # 向其他责任节点请求文件内容
                op_signal = self.request_file_from_responsible_nodes(responsible_nodes, HyDFSfilename)
                if op_signal is None:
                    logging.warning(f"Failed to retrieve {HyDFSfilename} from responsible nodes.")
                    return False
                
                # 请求成功后，复制 storage 中的文件夹到缓存
                if os.path.exists(cache_path):
                    shutil.rmtree(cache_path)  # 如果缓存中已存在该文件夹，先删除
                shutil.copytree(storage_path, cache_path)
                logging.info(f"Copied {HyDFSfilename} from storage to cache after retrieving from responsible nodes.")

        
        # 将缓存中的文件内容按顺序追加到 localfilename 文件中
        self.append_content_to_file(self.read_folder_content(cache_path), localfilename)
        # 更新缓存日志
        self.update_cachelog(HyDFSfilename, "READ")
        return True

    def read_folder_content(self, folder_path):
        """
        按特定顺序读取指定文件夹中的所有文件内容并返回合并的内容。
        读取顺序：origin 文件最先，剩余文件按 node_id 及 timestamp 排序。
        :param folder_path: 要读取的文件夹路径
        :return: 合并的文件内容（bytes）
        """
        # 获取文件夹中所有文件列表
        files = os.listdir(folder_path)

        # 定义排序规则
        def sort_key(filename):
            # 处理 origin 文件，使其排在最前
            if filename == "original" or filename.startswith("original"):
                return (0, "", 0)
            
            # 假设文件名格式为 "append_{timestamp}_{node_id}{extension}"
            parts = filename.split('_')
            if len(parts) >= 3 and parts[0] == "append":
                timestamp = int(parts[1])  # 提取时间戳并转换为整数
                node_id = parts[2].split('.')[0]  # 提取 node_id，去掉文件扩展名
                return (1, node_id, timestamp)
            else:
                return (2, filename, 0)  # 如果文件名格式不符，则排在最后

        # 按定义的排序规则对文件名进行排序
        files_sorted = sorted(files, key=sort_key)

        # 逐个读取文件内容并合并
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
        """
        将给定内容追加到指定文件的末尾。
        :param content: 要追加的内容（bytes）
        :param target_file: 目标文件路径
        """
        # 检查目标文件是否存在，如果不存在则创建
        if not os.path.exists(target_file):
            open(target_file, 'wb').close()

        # 将内容追加到文件末尾
        with open(target_file, 'ab') as f:
            f.write(content)
        logging.info(f"Appended content to {target_file}")

    def update_cachelog(self, filename, operation):
        with self.cachelog_lock:
            self.cachelog[filename] = operation
       

    def create_file(self, localfilename, HyDFSfilename):
        """Creates a file and distributes it to the responsible nodes."""
        # Check if file already exists globally
        if HyDFSfilename in self.membership_node.get_global_file_list():
            print(f"File {HyDFSfilename} already exists in the distributed system. Creation failed.")
            return False

        # Check if local file exists
        if not os.path.exists(localfilename):
            print(f"Local file {localfilename} does not exist.")
            return False

        # Determine responsible nodes and store locally if necessary
        responsible_nodes = self.get_responsible_nodes(HyDFSfilename)
        
        # Update global file list in membership_node
        self.membership_node.update_global_file_list(HyDFSfilename)
        logging.info(f"list is {list(self.membership_node.get_global_file_list())}")
        # Distribute the file to responsible nodes
        self.distribute_file(HyDFSfilename, localfilename, responsible_nodes)
        return True

    def distribute_file(self, HyDFSfilename, localfilename, responsible_nodes):
        """Distributes the file to its responsible nodes, excluding the current node if applicable."""
        for node_id, node_info in responsible_nodes.items():
            if node_id != self.node_id:
                # 传递节点信息用于发送文件
                self.send_file_to_node(node_info, HyDFSfilename, localfilename)
            else:
                self.send_file_local(HyDFSfilename, localfilename)

    def send_file_to_node(self, target_node, HyDFSfilename, file_path):
        """主动向目标节点发送文件内容"""
        target_ip = target_node['ip']
        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
                # 创建报头并发送
                header = self.create_header("SEND_FILE", HyDFSfilename)
                s.sendall(header)

                # 读取并发送文件内容
                with open(file_path, 'rb') as f:
                    while chunk := f.read(4096):  # 每次发送 4KB 数据
                        s.sendall(chunk)

        except (socket.timeout, socket.error) as e:
            print(f"Error sending file {HyDFSfilename} to {target_ip}: {e}")

    def send_file_local(self, HyDFSfilename, localfilename):
        """Stores the file locally by copying it from the current directory to the HyDFS storage."""
        file_dir = os.path.join(self.storage_dir, HyDFSfilename)
        os.makedirs(file_dir, exist_ok=True)

        # 使用 HyDFSfilename 提取文件后缀并生成文件路径
        file_extension = os.path.splitext(HyDFSfilename)[1]  # 提取文件后缀
        original_path = os.path.join(file_dir, "original" + file_extension)

        # base copy of file naming original
        with open(localfilename, 'rb') as src_file:
            with open(original_path, 'wb') as dest_file:
                dest_file.write(src_file.read())

        # Lock to ensure thread-safe updates to file_store
        with self.rw_file_store_lock.acquire_write():
            self.file_store.add(HyDFSfilename)

    
        
    def _hash(self, key, prefix=""):
        """
        生成一致性哈希值 用于文件名或节点ID的映射.
        :param key: 文件或节点的唯一标识符
        :param prefix: 区分文件和节点的前缀 (例如 "file-" 或 "node-")
        """
        return int(hashlib.sha1((prefix + key).encode()).hexdigest(), 16)

    def get_responsible_nodes(self, filename):
        """
        获取负责存储该文件的节点列表（基于一致性哈希），返回格式为:
        {node_id: {'ip': <ip_address>, 'incredible': <bool>}, ...}
        """
        file_hash = self._hash(filename, prefix="file-")  # 为文件生成哈希值
        membership_list = self.membership_node.get_membership_list()  # 获取完整的节点信息字典
        hash_ring = {self._hash(node_id, prefix="node-"): node_id for node_id in membership_list.keys()}
        sorted_hashes = sorted(hash_ring.keys())

        primary_index = 0
        for i in range(len(sorted_hashes)):
            if sorted_hashes[i] >= file_hash:
                primary_index = i
                break
        else:
            primary_index = 0  # 如果文件哈希值大于所有节点的哈希值，环回到第一个节点

        responsible_nodes = {}
        for i in range(self.replication_factor):
            responsible_index = (primary_index + i) % len(sorted_hashes)
            responsible_node_id = hash_ring[sorted_hashes[responsible_index]]
            
            # 直接使用 membership_list 中已有的节点信息
            responsible_nodes[responsible_node_id] = membership_list[responsible_node_id]
        return responsible_nodes


### clean_up
def delete_folder(folder_name):
    """删除指定文件夹及其内容"""
    if os.path.isdir(folder_name):
        shutil.rmtree(folder_name)
        print(f"{folder_name} 文件夹及其内容已删除.")
    else:
        print(f"{folder_name} 文件夹不存在.")

def clear_file(file_name):
    """清空指定文件内容"""
    if os.path.isfile(file_name):
        with open(file_name, 'w'):
            pass  # 清空文件内容
        print(f"{file_name} 文件内容已清空.")
    else:
        print(f"{file_name} 文件不存在.")

def clean_folder():
    # 删除 hydfs_storage 和 hydfs_cache 文件夹
    delete_folder("hydfs_storage")
    delete_folder("hydfs_cache")
    
    # 清空 local_log.log 文件内容
    clear_file("local_log.log")

def encodeid(ip, port, incarnation, node_id):
    return f"vm{getvmnum()[-1]}#{ip[-2:]}{str(incarnation)[-3:]}"

# 启动测试程序
if __name__ == "__main__":
    # 定义候选节点 IP 列表（在实际测试中使用真实 IP 地址或 localhost）
    clean_folder()
    nodeId = int(getvmnum())
    candidate_list = ['172.22.157.76', '172.22.159.77', '172.22.95.76']  # 候选节点 IP 列表
    # 启动 membership node
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
    # 启动 HyDFS node
    hydfs_node = HyDFSNode(node_id=newNodeId, membership_node=membership_node)
    
    # 保持主线程运行，防止程序退出
    while True:
        time.sleep(10)

    
