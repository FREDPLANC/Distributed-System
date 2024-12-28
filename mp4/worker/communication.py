import json
import socket
import threading
from worker.task_executor import TaskExecutor
from worker.state_manager import StateManager
from common.membership_node import Node
from common.hydfs_node import HyDFSNode

class WorkerServer:
    def __init__(self, worker_id, host, port, membership_node, hydfsnode, executor=None, state_manager=None):
        self.executor: TaskExecutor= executor
        self.state_manager = state_manager
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.membership_node = membership_node
        self.hydfsnode = hydfsnode

    # def handle_task(self, task_request):
    #     task_id = task_request["task_id"]
    #     operator = task_request["operator"]
    #     data = task_request["data"]
    #     result = self.executor.execute(task_id, operator, data)
    #     print(f"Task {task_id} result: {result}")

    def start_worker_server(self, worker_id):
        while True:
            client_socket, client_address = self.server_socket.accept()
            print(f"Connection from {client_address}")
            threading.Thread(target=self.handle_task_request, args=(client_socket,)).start()

    
    def handle_task_request(self, client_socket):
        try:
            data = client_socket.recv(4096).decode("utf-8")
            if data:
                task_request = json.loads(data)
                print(f"Received task: {task_request}")
                self.executor.execute(task_request)
                # self.report_task_status(task_request["task_id"], "completed", result)
        finally:
            client_socket.close()
