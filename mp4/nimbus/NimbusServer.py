import socket
import json
import threading
from common.globl import HOSTLIST, TOPOLOGY_PORT, WOKER_PORT,NIMBUS_RECIEVE_PORT
class NimbusServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        # self.workers = workers

        print(f"Nimbus Server running on {self.host}:{self.port}")

    def collect_data_server(self, outputfile):
        """
        """
        recv_data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        recv_data_socket.bind((self.host, NIMBUS_RECIEVE_PORT))
        while True:
            client_socket, client_address = self.server_socket.accept()
            recv_data = client_socket.recv(4096).decode("utf-8")
            recv_data = json.loads(recv_data)
            print(recv_data)
            outputfile.writelines(line + '\n' for line in recv_data)

            # threading.Thread(target=self.handle_worker_report, args=(client_socket,)).start()
    def broadcast_topology(self, topology, workers):
        """
        """
        # print(workers)
        for workerid, address in workers.items():
            ip = address["ip"]
            port = TOPOLOGY_PORT  
            print(f"Broadcasting {topology} to {ip}:{port}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((ip, port))
                sock.sendall(json.dumps(topology).encode("utf-8"))


                # print(f"Topology sent to {ip}:{port}")
    # def handle_worker_report(self, client_socket):
    #     """
    #     """
    #     try:
    #         data = client_socket.recv(4096).decode("utf-8")
    #         if data:
    #             report = json.loads(data)
    #             print(f"Received report: {report}")

    #             if report["type"] == "task_status":
    #                 self.process_task_status(report)
    #     finally:
    #         client_socket.close()

    # def process_task_status(self, report):
    #     """

    #     """
    #     task_id = report["task_id"]
    #     status = report["status"]
    #     result = report.get("result")
    #     print(f"Task {task_id} status: {status}")
    #     if status == "completed":
    #         print(f"Task result: {result}")

    def send_task(self, worker_id, worker_ip, port, task: json):
        """
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((worker_ip, WOKER_PORT))
            sock.sendall(json.dumps(task).encode("utf-8"))
            print(f"Task {task['task_id']} sent to {task}:{port}")
