import json
import socket
from threading import Thread
import threading
from common.globl import HOSTLIST, TOPOLOGY_PORT
from worker.task_executor import TaskExecutor
class WorkerNode():
    def __init__(self, worker_id, worker_ip, worker_port, hydfs_node, membership_node):
        self.worker_id = worker_id
        self.worker_ip = worker_ip
        self.worker_port = int(worker_port)
        self.task = []
        # self.task.append(task)
        self.hydfs_node = hydfs_node
        self.membership_node = membership_node

        self.taskpool = {}
        # self.executor: TaskExecutor= TaskExecutor(membership_node, hydfs_node)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.worker_ip, int(self.worker_port)))
        self.server_socket.listen(20)
        
        self.topo_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.topo_server_socket.bind((self.worker_ip, TOPOLOGY_PORT))
        self.topo_server_socket.listen(20)

        threading.Thread(target=self.topo_update, args=(), daemon=True).start()
        threading.Thread(target=self.start_worker_server, args=(worker_id,), daemon=True).start()

    def __str__(self):
        return "WorkerNode(worker_id={}, worker_ip={}, worker_port={})".format(
            self.worker_id, self.worker_ip, self.worker_port)

    def start_worker_server(self, worker_id):
        while True:
            client_socket, client_address = self.server_socket.accept()
            data = client_socket.recv(4096).decode("utf-8")
            # print(f"Connection from {client_address}")
            if data:
                task_request = json.loads(data)
                if task_request["data"]["order"] == 'create':
                    task_executor = TaskExecutor(self.membership_node, self.hydfs_node, task_request, self.worker_ip)
                    # self.taskpool[task_request["task_id"]] = task_executor
                    task_executor.execute(task_request)
                elif task_request["data"]["order"] == 'kill_':
                    #TODO: clean up the task
                    self.taskpool[task_request["task_id"]].clean_up()
                    self.taskpool.pop(task_request["task_id"])

    
    # def handle_task_request(self, client_socket):
    #     data = client_socket.recv(4096).decode("utf-8")
    #     if data:
    #         task_request = json.loads(data)
    #         print(f"Received task: {task_request}")
    #         self.executor.execute(task_request)
    #         # self.report_task_status(task_request["task_id"], "completed", result)


    def topo_update(self):
        while True:
            print("Waiting for topology update")
            client_socket, client_address = self.topo_server_socket.accept()
            for task_executor in self.taskpool.values():
                task_executor.set_topology(json.loads(client_socket.recv(4096).decode("utf-8")))

            # self.executor.set_topology(json.loads(client_socket.recv(4096).decode("utf-8")))
            # print(f"Connection from {client_address}")
            # threading.Thread(target=self.handle_task_request, args=(client_socket,)).start()

        



    # def __repr__(self):
    #     return self.__str__()
    # def assign_task(self, task):
    #     self.task = task

    # def add_parent_node(self, parent_node):
    #     self.parent_nodes.append(parent_node)
    
    # def add_child_node(self, child_node):
    # #     self.child_nodes.append(child_node)

    # def execute_task(self):
    #     if self.task:
    #         # Execute the assigned task
    #         pass