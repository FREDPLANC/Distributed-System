import threading
import time
import socket
import json

class MembershipNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.membership_list = {}  # 格式: {node_id: {'ip': ip_address, 'last_heartbeat': timestamp}}
        self.candidate_list = ['172.22.157.76', '172.22.159.77', '172.22.95.76']  # 候选节点 IP 列表
        self.self_ip = self.candidate_list[int(str((socket.gethostname()).split('-')[-1].split('.')[0][1:])) - 1]   # 当前节点的 IP 地址
        self.membership_list[self.self_ip] = {'ip': self.self_ip, 'last_heartbeat': time.time()}
        self.candidate_list = [ip for ip in self.candidate_list if ip != self.self_ip]  # 排除自身 IP
        self.ping_interval = 3  # 每 5 秒 ping 一次
        self.global_file_list = set()  # 全局文件列表
        threading.Thread(target=self.ping_candidates, daemon=True).start()
        threading.Thread(target=self.start_ping_response_server, daemon=True).start()
    
    def start_ping_response_server(self, port=10001):
        """启动一个简单的服务器来响应 PING 请求并返回 global_file_list"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind(('0.0.0.0', port))

        while True:
            data, address = server_socket.recvfrom(4096)
            threading.Thread(target=self.handle_ping_request, args=(server_socket, data, address), daemon=True).start()

    def handle_ping_request(self, server_socket, data, address):
        """处理来自其他节点的 PING 请求"""
        try:
            request = data.decode()
            if request == "PING":
                # 发送 PONG 响应
                server_socket.sendto(b"PONG", address)

                # 发送当前节点的 global_file_list
                global_file_list_data = json.dumps(list(self.global_file_list)).encode()
                server_socket.sendto(global_file_list_data, address)
        except Exception as e:
            print(f"Error handling PING request from {address}: {e}")

    def ping_candidates(self):
        """定期 ping 候选节点，检查是否可达并更新 membership_list，同时交换 global_file_list"""
        while True:
            for ip in self.candidate_list:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                        s.settimeout(2)
                        s.sendto(b"PING", (ip, 10001))
                        s.sendto(json.dumps(list(self.global_file_list)).encode(), (ip, 10001))

                        response, _ = s.recvfrom(1024)
                        if response == b"PONG":
                            # 接收对方的 global_file_list 并更新
                            file_list_data, _ = s.recvfrom(4096)
                            if file_list_data:
                                remote_file_list = set(json.loads(file_list_data.decode()))
                                self.global_file_list.update(remote_file_list)

                            self.membership_list[ip] = {'ip': ip, 'last_heartbeat': time.time()}
                            
                except (socket.timeout, socket.error):
                    
                    if ip in self.membership_list:
                        del self.membership_list[ip]  # 移除不可达的节点
            time.sleep(self.ping_interval)

    def get_membership_list(self):
        """返回当前的 membership list"""
        return self.membership_list

    def update_global_file_list(self, filename):
        """维护一个简单的全局文件列表"""
        self.global_file_list.add(filename)

    def get_global_file_list(self):
        """返回当前的全局文件列表"""
        return self.global_file_list