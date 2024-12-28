import socket
from worker.task_executor import TaskExecutor
from common.globl import HOSTLIST, TOPOLOGY_PORT, WOKER_PORT
from common.hydfs_node import HyDFSNode, clean_folder, getvmnum, encodeid
from common.membership_node import Introducer, Node
from worker.Worker import WorkerNode
import time

def main():
    clean_folder()
    worker_id = socket.gethostname().split('.')[0]
    vm1 = HOSTLIST[0]
    port = '9999'
    ip = HOSTLIST[int(getvmnum()[-1:]) - 1]

    incarnation = int(time.time() * 1000)
    newNodeId = encodeid(ip, port, incarnation, f'{ip}:{port}#{incarnation}')
    membership_node = Node(ip,'9999', vm1+":"+port, 'PingAck', 0, incarnation=incarnation)
    time.sleep(1)
    hydfs_node = HyDFSNode(newNodeId, membership_node)

    woker = WorkerNode(worker_id, ip, WOKER_PORT, hydfs_node, membership_node)
    
    woker.start_worker_server(worker_id)

    # start_worker_server(executor)
    while True:
        time.sleep(10)

    # def monitor(self):
    #     while True:
            
            # print("Worker is running")

if __name__ == "__main__":
    main()
