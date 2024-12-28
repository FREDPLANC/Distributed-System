from nimbus.Nimbus import Nimbus
from common.globl import HOSTLIST, TASKIDMAP
from common.hydfs_node import HyDFSNode, clean_folder, getvmnum, encodeid
from common.membership_node import Introducer, Node


import time
import os







if __name__ == "__main__":
    clean_folder()
    incarnation = int(time.time() * 1000)
    # if getvmnum() == '001':
    membership_node = Introducer(HOSTLIST[0], '9999', 'PingAck', 0, incarnation=incarnation)
    # else:
    #     membership_node = Node(hostlist[int(getvmnum()[-1:])-1],'9999', hostlist[0]+":"+'9999', 'PingAck', 0, incarnation=incarnation)
    time.sleep(1)
    ip = HOSTLIST[int(getvmnum()[-1:]) - 1]
    port = '9999'
    node_id =f'{ip}:{port}#{incarnation}'
    newNodeId = encodeid(ip, port, incarnation, node_id)
    hydfs_node = HyDFSNode(newNodeId, membership_node)
    # create journals and logs prehead to prepare the tasks
    while True:
        ipt = input("Enter: <op1_exe> <op2_exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>:\n $$ ").split()
        if len(ipt) == 5:
            op1_exe, op2_exe, hydfs_src_file, hydfs_dest_filename, num_tasks = ipt
            break
        else:
            print("Invalid input")
    nimbus = Nimbus(op1_exe, op2_exe, hydfs_src_file, hydfs_dest_filename, num_tasks, hydfs_node, membership_node)
    while True:
        time.sleep(10)
        time.sleep(10)
    # results = nimbus.start()
    # print("Results:", results)


