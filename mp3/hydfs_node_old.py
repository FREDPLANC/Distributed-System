import hashlib
import time
from collections import defaultdict
import threading
from membership_node import MembershipNode  # 引入已有的 membership_node 类
from rw_lock import ReadWriteLock
from base36encode import AppendIdentifier
import os
import struct
import socket
import logging
import time
import base64


logging.basicConfig(filename='local_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def getvmnum():
    hostname = socket.gethostname()
    return str(hostname.split('-')[-1].split('.')[0][1:])

class HyDFSNode:
    def __init__(self, node_id, membership_node, storage_dir="hydfs_storage", cache_dir="hydfs_cache"):
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

        # thread task 1
        self.scan_interval = 10  # 定期检查的时间间隔（秒）
        threading.Thread(target=self.start_periodic_scan, daemon=True).start()  # 启动扫描线程
        
        # thread task 2
        self.filePort = 10000
        threading.Thread(target=self.file_exchange_server, args=(self.filePort,), daemon=True).start()
        # self.file_exchange_server(self.filePort) # start another thread to receive file change task


        # thread task 3
        command_thread = threading.Thread(target=self.listen_for_commands, daemon=True).start()
        
        # lock
        self.rw_file_store_lock = ReadWriteLock() # lock both responsible and store
        self.cachelog_lock = threading.Lock()
    
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
                else:
                    print("Invalid command. Use 'create localfilename HyDFSfilename' or 'get HyDFSfilename localfilename'.")
            except Exception as e:
                print(f"Error processing command: {e}")
    
    def file_exchange_server(self, port=10000):
        """start listening server at port"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(5) # 最大连接数为 5

        # whenever a new connection is made, a new thread is created to handle the file request
        while True:
            client_socket, address = server_socket.accept() # blocking call
            threading.Thread(target=self.handle_file_request, args=(client_socket, address), daemon=True).start()

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
                    self.send_append_to_node(node_info, header + file_content)
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
        timestamp = int(time.time() * 10)
        placeholder = f"{timestamp}_{self.node_id[-1]}".ljust(20)[:20]  # 20 字节

        # 使用 struct 打包定长报头
        header = struct.pack(f'20s 100s Q 20s', 
                             operation_type_field.encode(), 
                             filename_field.encode(), 
                             time_vector_field, 
                             placeholder.encode())
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
        try:
            timestamp_str, node_id_str = placeholder.split("_")
            timestamp = int(timestamp_str)
            node_id = int(node_id_str)
        except ValueError:
            logging.error("Invalid placeholder format.")
            return None

        return operation_type, filename, timestamp, node_id

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
                file_path = os.path.join(self.storage_dir, filename, "original" + os.path.splitext(filename)[1])
                if os.path.exists(file_path):
                    with open(file_path, 'rb') as f:
                        while chunk := f.read(4096):
                            client_socket.sendall(chunk)
                    logging.info(f"Sent file {filename} to {address}")
                else:
                    client_socket.sendall(b"ERROR: File not found")

            elif operation_type == "SEND_FILE":
                # 接收并存储文件
                file_dir = os.path.join(self.storage_dir, filename)
                os.makedirs(file_dir, exist_ok=True)
                file_path = os.path.join(file_dir, "original" + os.path.splitext(filename)[1])

                with open(file_path, 'wb') as f:
                    while chunk := client_socket.recv(4096):
                        f.write(chunk)
                logging.info(f"Received file {filename} from {address}")

            elif operation_type == "APPEND_FILE":
                # 检查目标文件夹是否存在
                file_dir = os.path.join(self.storage_dir, filename)
                if not os.path.exists(file_dir):
                    logging.error(f"Directory for file {filename} does not exist. Aborting append.")
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

            file_content = self.request_file_from_node(node_info, HyDFSfilename)
            
            # (暂时）如果成功接收文件内容，返回该内容
            if file_content:
                return file_content

        return None


    def request_file_from_node(self, target_node, HyDFSfilename):
        """向目标节点发送文件请求并接收文件内容"""
        target_ip = target_node['ip']

        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
                # 创建并发送包含操作类型的报头
                header = self.create_header("REQUEST_FILE", HyDFSfilename)
                s.sendall(header)

                # 接收响应的文件内容
                file_content = bytearray()
                while True:
                    chunk = s.recv(4096)  # 每次接收 4KB 数据
                    if not chunk:
                        break
                    file_content.extend(chunk)

                if file_content:
                    logging.info(f"Successfully received {HyDFSfilename} from {target_ip}")
                    with self.rw_file_store_lock.acquire_write():
                        self.file_store.add(HyDFSfilename)
                    logging.info(f"add {HyDFSfilename} to file_store ")
                    return file_content

                

        except (socket.timeout, socket.error) as e:
            print(f"Error requesting file {HyDFSfilename} from {target_ip}: {e}")
            return None
        
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


    def get_file(self, HyDFSfilename, localfilename):
        flag = 0

        with self.cachelog_lock:
            if HyDFSfilename in self.cachelog and self.cachelog[HyDFSfilename] == "READ":
                flag = 1
        
        # 根据 cachelog 判断是否可以直接从缓存读取
        cache_path = os.path.join(self.cache_dir, HyDFSfilename)
        if flag == 1 and os.path.exists(cache_path):
            file_content = self.read_files(cache_path)  # 从缓存中读取文件内容
        else:
            # 获取负责节点列表
            responsible_nodes = self.get_responsible_nodes(HyDFSfilename)

            # 如果当前节点是责任节点，直接读取文件内容
            if self.node_id in responsible_nodes.keys():
                file_content = self.read_files(os.path.join(self.storage_dir, HyDFSfilename))
            else:
                # 向其他责任节点请求文件内容
                file_content = self.request_file_from_responsible_nodes(responsible_nodes, HyDFSfilename)
                if file_content is None:
                    return False

            # 将文件内容写入到缓存目录
            with open(cache_path, 'wb') as cache_file:
                cache_file.write(file_content)

        # 将文件内容写入到 localfilename 路径
        with open(localfilename, 'wb') as local_file:
            local_file.write(file_content)

        self.update_cachelog(HyDFSfilename, "READ")
        return True

    def read_files(self, filepath):
        """读取文件或文件夹下的所有文件并拼接成完整内容"""
        if os.path.isfile(filepath):
            # 如果是单一文件，直接读取内容
            with open(filepath, 'rb') as f:
                return f.read()

        elif os.path.isdir(filepath):
            # 如果是文件夹，读取所有文件并按名称排序拼接
            chunks = sorted(f for f in os.listdir(filepath))  # 不限定后缀，读取所有文件
            file_content = bytearray()
            for chunk_file in chunks:
                chunk_path = os.path.join(filepath, chunk_file)
                if os.path.isfile(chunk_path):  # 确保只读取文件，不处理子文件夹
                    with open(chunk_path, 'rb') as chunk:
                        file_content.extend(chunk.read())

            return file_content

        return None

    

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


    def receive_replica(self, HyDFSfilename, file_content):
        pass


    def start_periodic_scan(self): 
        while True:
            self.check_and_update_responsibilities()
            time.sleep(self.scan_interval)
        
        

    def check_and_update_responsibilities(self):
        global_file_list = self.membership_node.get_global_file_list()  # 获取最新的全局文件列表

        for filename in global_file_list:
            expected_responsible_nodes = self.get_responsible_nodes(filename)  # 获取负责的节点列表

            if self.node_id in expected_responsible_nodes:
                # Lock to ensure thread-safe updates to file_store
                with self.rw_file_store_lock.acquire_read():
                    if filename in self.file_store:
                        continue

                # 获取主节点（第一个责任节点）
                primary_node_id = next(iter(expected_responsible_nodes))  # 获取字典中的第一个键

              
                if self.node_id == primary_node_id:
                    # 当前节点是主节点，需要向下一个节点请求文件副本
                    sorted_node_ids = list(expected_responsible_nodes.keys())
                    next_node_id = sorted_node_ids[1]  # 获取下一个节点 ID
                    
                    self.request_file_from_node(expected_responsible_nodes[next_node_id], filename)
                else:
                    # 当前节点不是主节点，向主节点请求文件副本
                    self.request_file_from_node(expected_responsible_nodes[primary_node_id], filename)



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


# 启动测试程序
if __name__ == "__main__":
    # 定义候选节点 IP 列表（在实际测试中使用真实 IP 地址或 localhost）
    nodeId = int(getvmnum())
    candidate_list = ['172.22.157.76', '172.22.159.77', '172.22.95.76']  # 候选节点 IP 列表
    # 启动 membership node
    membership_node = MembershipNode(node_id=candidate_list[nodeId-1])

    # 启动 HyDFS node
    hydfs_node = HyDFSNode(node_id=candidate_list[nodeId-1], membership_node=membership_node)
    
    # 保持主线程运行，防止程序退出
    while True:
        time.sleep(10)

    
