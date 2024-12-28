import os
import socket
import threading
import multiprocessing
import time

# 创建一个类 HyDFSNode，用于模拟发送和接收文件

class HyDFSNode:
    def __init__(self, node_id, storage_dir="hydfs_storage"):
        self.node_id = node_id
        self.storage_dir = storage_dir
        self.filePort = 10000  # 固定端口进行文件传输
        os.makedirs(self.storage_dir, exist_ok=True)

        # 启动文件交换服务器线程
        threading.Thread(target=self.file_exchange_server, args=(self.filePort,), daemon=True).start()

    def file_exchange_server(self, port=10000):
        """start listening server at port"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(5)

        print(f"Server listening on port {port}...")
        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_file_request, args=(client_socket, address), daemon=True).start()

    def handle_file_request(self, client_socket, address):
        """Handle incoming file requests."""
        try:
            request = client_socket.recv(1024).decode()
            if request.startswith("REQUEST_FILE"):
                filename = request.split(" ")[1]
                file_path = os.path.join(self.storage_dir, filename, "original" + os.path.splitext(filename)[1])
                if os.path.exists(file_path):
                    with open(file_path, 'rb') as f:
                        while chunk := f.read(4096):
                            client_socket.sendall(chunk)
                    print(f"Sent file {filename} to {address}")
                else:
                    client_socket.sendall(b"ERROR: File not found")

            elif request.startswith("SEND_FILE"):
                filename = request.split(" ")[1]
                file_dir = os.path.join(self.storage_dir, filename)
                os.makedirs(file_dir, exist_ok=True)
                file_path = os.path.join(file_dir, "original" + os.path.splitext(filename)[1])
                with open(file_path, 'wb') as f:
                    while chunk := client_socket.recv(4096):
                        if not chunk:
                            break
                        f.write(chunk)
                print(f"Received file {filename} from {address}")

        except Exception as e:
            print(f"Error handling file request from {address}: {e}")
        finally:
            client_socket.close()

    def send_file_to_node(self, target_ip, HyDFSfilename, file_path):
        """Send file to target node."""
        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
                s.sendall(f"SEND_FILE {HyDFSfilename}".encode())
                with open(file_path, 'rb') as f:
                    while chunk := f.read(4096):
                        s.sendall(chunk)
                print(f"File {HyDFSfilename} sent to {target_ip}")
        except (socket.timeout, socket.error) as e:
            print(f"Error sending file {HyDFSfilename} to {target_ip}: {e}")

    def request_file_from_node(self, target_ip, HyDFSfilename):
        """Request file from target node."""
        try:
            with socket.create_connection((target_ip, self.filePort), timeout=10) as s:
                s.sendall(f"REQUEST_FILE {HyDFSfilename}".encode())
                file_content = bytearray()
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    file_content.extend(chunk)

                if file_content:
                    file_path = os.path.join(self.storage_dir, HyDFSfilename, "received_original")
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    with open(file_path, 'wb') as f:
                        f.write(file_content)
                    print(f"Successfully received {HyDFSfilename} from {target_ip}")
                else:
                    print(f"File {HyDFSfilename} not found on {target_ip}")

        except (socket.timeout, socket.error) as e:
            print(f"Error requesting file {HyDFSfilename} from {target_ip}: {e}")


# 本地测试逻辑
if __name__ == "__main__":
    # 启动接收端进程
    receiver_node = multiprocessing.Process(target=HyDFSNode, args=(1,))
    receiver_node.start()

    # 等待接收端服务器启动
    time.sleep(2)

    # 启动发送端（主进程模拟）
    sender_node = HyDFSNode(2)
    test_file_path = "test_file.txt"

    # 发送文件到接收端
    sender_node.send_file_to_node('127.0.0.1', 'test_file.txt', test_file_path)

    # 请求文件从接收端获取
    sender_node.request_file_from_node('127.0.0.1', 'test_file.txt')

    # 终止接收端进程
    receiver_node.terminate()
    receiver_node.join()