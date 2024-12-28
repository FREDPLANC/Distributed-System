import atexit
import hashlib
import subprocess
import time
from collections import defaultdict
import threading
from common.membership_node import Node, Introducer
from common.rw_lock import ReadWriteLock
from common.base36encode import AppendIdentifier
from common.run import runner, output_to_console
import logging
import time
from common.run import runner, output_to_console, create_journals_and_logs
from common.globl import HOSTLIST, STAGE_NAMES, NIMBUS_PORT, WOKER_PORT, TASKIDMAP

from worker.Worker import WorkerNode
from nimbus.NimbusServer import NimbusServer
import os
#TODO: test topology management by create a thread and input command

class Nimbus:
    def __init__(self, op1, op2, hydfs_src_file, hydfs_dest_filename, num_tasks, hydfs_node=None,membership_node=None):
        self.nodes = []
        self.tasks = {}
        self.generate_tasks(3)
        self.topology = {}   #
        self.hydfs_node = hydfs_node
        self.membership_node: Node = membership_node
        self.workers = {} #dic {workerid, {ip, port}}
        self.current_workers = set() #set of workerid
        self.workloads = {} #dic {workerid, load}
        self.task_to_worker = {} #dic {taskid, workerid}
        self.op1 = op1
        self.op2 = op2
        self.hydfs_src_file = hydfs_src_file
        self.hydfs_dest_filename = hydfs_dest_filename
        self.num_tasks = num_tasks

        # memlist = self.membership_node.get_memberlist() #TODO: need to exclude the introducer
        memlist = {"vm1#2131":{'a':1, 'b':2, 'c':3}, "vm2#2132":{'a':1, 'b':2, 'c':3}, "vm3#2133":{'a':1, 'b':2, 'c':3}, 
                   "vm4#2134":{'a':1, 'b':2, 'c':3}, "vm5#2135":{'a':1, 'b':2, 'c':3}, "vm6#2136":{'a':1, 'b':2, 'c':3},
                     "vm7#2137":{'a':1, 'b':2, 'c':3}, "vm8#2138":{'a':1, 'b':2, 'c':3}, "vm9#2139":{'a':1, 'b':2, 'c':3},}
        
        
        self.NimbusServer = NimbusServer(HOSTLIST[0], NIMBUS_PORT)
        
        threading.Thread(target=self.monitor_user_input).start()
        threading.Thread(target=self.failDetect).start()
        while not self.current_workers:
            time.sleep(0.4)
            self.initialize_workers()
        
        self.send_out_init_tasks()
        # for worker in self.current_workers:
        #     self.workloads[worker] = 0
        # self.topology = self.generate_topology(3, self.workers)
        # self.update_workers()

        
        # self.topology = self.generate_topology(3)
        # self.testpart()


        #sleep for a while to wait for the workers to join

        threading.Thread(target=self.update_workers).start()
        

        '''
        Topology structure:
        {'worker1':{'Task_Spout_1', 'Task_Spout_2', 'Task_Spout_3'}, 
        'worker2':{'Task_Bolt1_1', 'Task_Bolt1_2', 'Task_Bolt1_3'},
          'worker3':{'Task_Bolt2_1', 'Task_Bolt2_2', 'Task_Bolt2_3'}}

        #discard
        {'tasks': {'Task_Spout_1': 'vm1#2131', 'Task_Spout_2': 'vm2#2132', 'Task_Spout_3': 'vm3#2133'}}
        Bolt1
        {'tasks': {'Task_Bolt1_1': 'vm4#2134', 'Task_Bolt1_2': 'vm5#2135', 'Task_Bolt1_3': 'vm6#2136'}}
        Bolt2
        {'tasks': {'Task_Bolt2_1': 'vm7#2137', 'Task_Bolt2_2': 'vm8#2138', 'Task_Bolt2_3': 'vm9#2139'}}
        '''
    def send_out_init_tasks(self):
        
        for worker in self.workers:
            for task in self.topology[worker]:
                print("task:", task)
                json = self.generate_task_json(self.get_optype(task), 
                                    task, self.get_opertor(task), {'order': 'create'}, 
                                    self.task_to_worker)
                self.NimbusServer.send_task(worker, 
                                        self.workers[worker]["ip"], 
                                        self.workers[worker]["port"], 
                                        json)
        self.find_workers_for_tasks()
        self.NimbusServer.broadcast_topology(self.task_to_worker, self.workers)
        # print(self.topology)
    def generate_tasks(self, tasks_per_stage):
        for i,stage_name in STAGE_NAMES.items():
            # Assign tasks to workers
            self.tasks[stage_name] = set()
            for task_idx in range(1, tasks_per_stage + 1):
                task_name = f"Task_{stage_name}_{task_idx}"
                self.tasks[stage_name].add(task_name)
        print(self.tasks)
    def monitor_user_input(self):
        while True:
            command = input("Enter command: ")
            command = command.strip('#')
            if command and command[0] == "add":
                print("adding new worker")
                workerid = command[1]
                self.handle_new_node(workerid)
            elif command == "print":
                self.print_topology()
            elif command == "get":
                print(self.membership_node.get_alive_machine())
            else:
                print("Invalid command")
    
    def initialize_workers(self):
        """
        """
        
        alive_workers = self.membership_node.get_alive_machine()
        if len(alive_workers) < 9:
            # print("error")
            return
        for worker_id in alive_workers:
            ip = self.get_worker_ip(worker_id)
            self.workers[worker_id] = {"ip": ip, "port": WOKER_PORT}
            self.workloads[worker_id] = 1
            self.current_workers.add(worker_id)  
            # self.topology[worker_id] = []
            
        worker_index = 0  
        wokers = list(self.current_workers)
        for stage in self.tasks:
            for task in self.tasks[stage]:
                self.topology[wokers[worker_index]] = set()
                self.topology[wokers[worker_index]].add(task)
                worker_index += 1  
        
        self.find_workers_for_tasks()


        # for worker_id in alive_workers:
        #     self.handle_new_node(worker_id)

    def get_worker_ip(self, worker_id):
        """
        """
        worker_index = int(worker_id.split("#")[0][-1]) - 1
        return HOSTLIST[worker_index]

    def update_workers(self):
        # Check if any worker has failed
        while True:
            alive_workers = self.membership_node.get_alive_machine()
            # memlist = self.membership_node.get_membership_list()
            # print("alive_workers:", alive_workers)
            # print("current_workers:", self.current_workers)
            failed_workers  = self.current_workers - alive_workers
            new_workers = alive_workers - self.current_workers
            if failed_workers:
                for worker in failed_workers:
                    print("failed_workers:", failed_workers)
                    print("workers topology", self.topology[worker])
                    self.handle_job_failure(worker)

            if new_workers:
                print("new_workers:", new_workers)
                for worker in new_workers:
                    self.handle_new_node(worker)

            time.sleep(0.1)


        # for 
    
    # def generate_topology(self, tasks_per_stage):
    #     topology = {}
    #     stage_names = {1: "Spout", 2: "Bolt1", 3: "Bolt2"}

    #     # for i,stage_name in stage_names.items():
    #     #     topology[stage_name] = {"tasks": {}}
    #     #     # Assign tasks to workers
    #     #     for task_idx in range(1, tasks_per_stage + 1):
    #     #         task_name = f"Task_{stage_name}_{task_idx}"

    #     #         assigned_worker = min(self.workers.keys(), key=lambda w: self.workloads[w])
    #     #         # worker_ids[worker_idx % num_workers]  # Round-robin
    #     #         topology[stage_name]["tasks"][task_name]=assigned_worker
    #     #         self.workloads[assigned_worker] += 1
    #     return topology
    
    def handle_job_failure(self, failed_worker):
        # print("active_workers:", active_workers)

        # for worker in failed_worker:
        self.workloads.pop(failed_worker, None)
        self.workers.pop(failed_worker, None)
        failed_tasks = self.topology[failed_worker].copy()
        target_mapping = {}
        print("failed_tasks:", failed_tasks)
        print(" ")
        if not self.workers :
            self.current_workers.remove(failed_worker)
            self.topology.pop(failed_worker, None)
            return
        
        for task in failed_tasks:
            print("failed_task:", task)
            new_worker = min(self.workers, key=lambda w: self.workloads[w])  #find the worker with the least load

            self.topology[new_worker].add(task)
            self.workloads[new_worker] += 1
            #TODO: send the task to the new worker

        self.current_workers.remove(failed_worker)
        self.topology.pop(failed_worker, None)
        self.find_workers_for_tasks()

        for task in failed_tasks:
            json = self.generate_task_json(self.get_optype(task), 
                                    task, self.get_opertor(task), {'order': 'create'}, 
                                    self.task_to_worker)
            self.NimbusServer.send_task(new_worker, 
                                        self.workers[new_worker]["ip"], 
                                        self.workers[new_worker]["port"], 
                                        json)
            self.NimbusServer.broadcast_topology(self.task_to_worker, self.workers)

    def createJournalAndLog(self, task_id):
        
        local_filename = f"{task_id}_log.log" 
            
        
        self.hydfs_node.create_file(local_filename, local_filename)
            
         
        if os.path.exists(local_filename):
            os.remove(local_filename)
    
        # create journal file
        local_filename = f"{task_id}_journal.log"
      
            
        self.hydfs_node.create_file(local_filename, local_filename)
            

        if os.path.exists(local_filename):
            os.remove(local_filename)



    def createJournalsAndLogs(self):
        # Create journals and logs
        create_journals_and_logs()
        for item in TASKIDMAP:
            self.createJournalAndLog(item)


    def handle_new_node(self, new_worker):

        self.workloads[new_worker] = 0
        self.workers[new_worker] = {"ip": self.get_worker_ip(new_worker), "port": WOKER_PORT}
        self.topology[new_worker] = []
        if not self.current_workers:
            self.workloads[new_worker] = len(self.tasks) * len(self.tasks["Spout"])
            for statge in self.tasks:
                for task in self.tasks[statge]:
                    self.topology[new_worker].add(task)
            
            #only one worker, maybe wait for a while to add more workers to start the tasks
        else:
            max_worker = max(self.workloads, key=self.workloads.get)
            new_task = self.topology[max_worker].pop()
            self.topology[new_worker].add(new_task)
            self.workloads[max_worker] -= 1
            self.workloads[new_worker] += 1
            self.find_workers_for_tasks()
            #TODO: tell old
            #TODO: tell new
            oldjson = self.generate_task_json(self.get_optype(new_task), 
                                    new_task, self.get_opertor(new_task), {'order': 'kill'}, 
                                    self.task_to_worker)
            
            newjson = self.generate_task_json(self.get_optype(new_task), 
                                    new_task, self.get_opertor(new_task), {'order': 'create'}, 
                                    self.task_to_worker, )
            self.NimbusServer.send_task(max_worker, 
                                        self.workers[max_worker]["ip"], 
                                        self.workers[max_worker]["port"], 
                                        oldjson)
            self.NimbusServer.send_task(new_worker, 
                                        self.workers[new_worker]["ip"], 
                                        self.workers[new_worker]["port"], 
                                        newjson)
            self.NimbusServer.broadcast_topology(self.task_to_worker, self.workers)

        self.current_workers.add(new_worker) 

    def find_workers_for_tasks(self):
        """
        """
        for stage in self.tasks:
            print("task:", stage)
            for task in self.tasks[stage]:
                for worker, tasks in self.topology.items():
                    print("worker:", worker)
                    if task in tasks:
                        print("task in tasks")
                        self.task_to_worker[task] = worker
                        break  # 找到后立即停止内层循环


    def testpart(self):
        print(self.workers)
        self.workers.pop("vm1#2131")
        self.handle_job_failure("vm1#2131")
        self.workers["vm1#2131"] = {"ip": HOSTLIST[0], "port": 12345}
        self.handle_new_node("vm1#2131")
        self.print_topology()

    def print_topology(self):
        print("#"*20)
        for key, value in self.topology.items():
            print("worker:", key,":")
            print(self.topology[key])
        print("workload: ",self.workloads)
        print("#"*20)


    def failDetect(self):
        
        try:
            intput, output = runner(self.hydfs_src_file, self.hydfs_dest_filename,self.op1, self.op2) #TODO: 
        except:
            self.update_workers()
        # self.createJournalsAndLogs()
        app = self.op1.split("_")[1]
        state = int(app[-1])
        output_to_console(state, intput, output, self.hydfs_dest_filename)
        self.createJournalsAndLogs()
            
            
       
  

    def get_opertor(self, task_id): #TODO:
        '''
        '''
        if "Spout" in task_id:
            return "count"
        elif "Bolt" in task_id:
            return "lower"
        else:
            return "sum"
    def get_optype(self, task_id): #TODO:
        '''
        '''
        if "Spout" in task_id:
            return "Spout"
        elif "Bolt1" in task_id:
            return "Transform"
        else:
            return "AggregateByKey"
        
    def generate_task_json(self, otype, task_id, operator, data, topology):
        '''
        task_request = {
            "type": "Transform",
            "task_id": "Task_Spout_1",
            "operator": "count",
            "data": {"oreder", "create"}
            "topology": {'Task_Sout_1': ['vm1#12345', 'vm2#12345'], 'Task_Bolt_1': ['vm3#12345', 'vm4#12345'], 'Task_Bolt_2': ['vm5#12345', 'vm6#12345']}
            "partition": (start, end)
            "source": "/home/twei11/ga0/mp4/hydfs_local_get",
            "dest": "/home/twei11/ga0/mp4/hydfs_local_get",
        }

        '''
        if task_id.split('_')[1] == 'Spout':
            partition = self.generate_partition(int(task_id.split('_')[2]), self.num_tasks)
        else:
            partition = None

        task_request = {
            "type": otype,
            "task_id": task_id,
            "operator": operator,
            "data": data,
            "topology": topology,
            "partition": partition,
            "source": self.hydfs_src_file,
            "dest": self.hydfs_dest_filename,
        }
        return task_request
    
    def generate_partition(self, task_id, num_tasks):
        '''
        '''
        # total_rows = int(self.hydfs_src_file.split('_')[2])
        total_rows = 100 #TODO: test
        total_tasks = int(num_tasks)
        
        base_rows_per_task = int(total_rows) // int(total_tasks)
        extra_rows = total_rows % total_tasks

        if task_id == 1:
            start = 1 
            end = base_rows_per_task + (1 if extra_rows > 0 else 0)
        elif task_id == 2:
            start = base_rows_per_task + (1 if extra_rows > 0 else 0) + 1
            end = start + base_rows_per_task-1 + (1 if extra_rows > 1 else 0) 
        else:
            start = base_rows_per_task * 2 + extra_rows + 1
            end = total_rows
        #the remaing only can be 0 1 2, then we can append 1 on task1 or task2 or both.

        return (start, end)
    
    

