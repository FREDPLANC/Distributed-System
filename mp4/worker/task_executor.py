import ast
import json
import socket
import subprocess
import threading
import time
from common.globl import HOSTLIST, HYDFS_LOCAL_DIR, STREAMING_PORT_DIC, ACK_PORT, NIMBUS_STREAM_IP, NIMBUS_STREAM_PORT
from common.hydfs_node import HyDFSNode
from common.ExpiringSet import ExpiringSet
from itertools import islice

import os 
import sys

class TaskExecutor:
    def __init__(self, membership_node, hydfs_node, data, ip):

        self.task_request = data
        self.ack_lock = threading.Lock()
        self.aggregate_state = {}     # 存储 AggregateByKey 的中间状态
        self.threadpool = {} #{task_id: {thread, stopthread}}
        self.topology = data['topology']
        self.membership_node = membership_node
        self.hydfs_node:HyDFSNode = hydfs_node
        self.history_tuples = ExpiringSet(2)
        self.history_lock = threading.Lock()
        self.waitingack = []
        self.aggregate_dict = {}
        self.aggregate_lock = threading.Lock()
        self.ip = ip
    
        self.ack_pending_queue = []


    def set_topology(self, topology):
        self.topology = topology
        
    def createJournalAndLog(self, task_id):
        topology = self.topology
        vmnum = topology[task_id]
        
        local_filename = f"{task_id}_log.log"
        if self.hydfs_node.CheckGlobalFileList(local_filename):
            return
        try:
           
            with open(local_filename, "w") as log_file:
                pass  
            
        
            self.hydfs_node.create_file(local_filename, local_filename)
            
        finally:
         
            if os.path.exists(local_filename):
                os.remove(local_filename)
    
        # create journal file
        local_filename = f"{task_id}_journal.log"
        if self.hydfs_node.CheckGlobalFileList(local_filename):
            return
        try:
         
            with open(local_filename, "w") as log_file:
                pass  
            
     
            self.hydfs_node.create_file(local_filename, local_filename)
            
        finally:
        
            if os.path.exists(local_filename):
                os.remove(local_filename)
                
    def updateJournal(self, timestamp, batch, task_id): 
        
        content = []
        
        topology = self.topology
        vmnum = topology[task_id]   
        
        hydfs_filename = f"{task_id}_journal.log"
        
 
        content.append(f"-100, {timestamp}\n")
        

        for tupl, tuplid in batch:

            tuple_str = json.dumps(tupl)
            content.append(f"{tuplid}, {tuple_str}")

        self.hydfs_node.appendContent("\n".join(content).encode("utf-8"), hydfs_filename)
        
        
    def updateLog(self, timestamp, batch): 
  
      
        task_batches = {}

        for tuplid in batch:
            # 解析 tuple id 获取 task_id
            print(tuplid)
            _, _, task_id, _ = tuplid.split("#")  # 假设格式为 vm#task_id#current

            if task_id not in task_batches:
                task_batches[task_id] = [] 

           
            task_batches[task_id].append(f"{timestamp}, {tuplid}")
        
    
        for task_id, content in task_batches.items():
            hydfs_filename = f"{task_id}_log.log"  
            self.hydfs_node.appendContent("\n".join(content).encode("utf-8"), hydfs_filename)
    

    def process_recovery(self, journal_file, log_file):
       
        history_set = set() 
        acked_set = set()  
        send_set = set()    
        
 
        if not os.path.exists(journal_file) or not os.path.exists(log_file):
            raise FileNotFoundError("Both journal_file and log_file must exist.")
        
        with open(journal_file, "r") as journal, open(log_file, "r") as log:
            journal_lines = journal.readlines()[::-1]
            log_lines = log.readlines()[::-1]    

            journal_index = 0
            log_index = 0

            while journal_index < len(journal_lines):
               
                block = []
                block_timestamp = None
                while journal_index < len(journal_lines):
                    line = journal_lines[journal_index].strip()
                    journal_index += 1
                    
                    if line.startswith("-100"):
                   
                        block_timestamp = float(line.split(", ")[1])
                        break
                    else:
    
                        tuplid, tuple_data = line.split(", ", 1)
                        block.append((tuplid, tuple_data))

   
                if all(tuplid in history_set for tuplid, _ in block):
                    print("All IDs in the block are in the history_set. Stopping.")
                    break

          
                for tuplid, _ in block:
                    history_set.add(tuplid)

    
                while log_index < len(log_lines):
                    log_line = log_lines[log_index].strip()
                    log_index += 1

             
                    log_parts = log_line.split(", ")
                    log_timestamp = float(log_parts[0])
                    log_id = log_parts[1]

                    if log_timestamp < block_timestamp:
                        break 
                    
                    acked_set.add(log_id)

             
                for tuplid, tuple_data in block:
                    if tuplid not in acked_set:
                        send_set.add((tuplid, tuple_data))

        return send_set

    def recoverProcess(self, task_id):
        
        topology = self.topology
        vmnum = topology[task_id]
        
        journal_filename = f"{task_id}_journal.log"
        self.hydfs_node.get_file(journal_filename, "recoveryJournal.log")
        log_filename = f"{task_id}_log.log"
        self.hydfs_node.get_file(log_filename, "recoveryLog.log")
        
        
        send_set = self.process_recovery("recoveryJournal.log", "recoveryLog.log")

        os.remove("recoveryLog.log")

        with open("recoveryJournal.log", "w") as f:
            f.truncate(0)

        with open("recoveryJournal.log", "a") as f:
            current_time = time.time()
            f.write(f"-100, {current_time}\n")  
            
            for tuplid, tuple_data in send_set:
                f.write(f"{tuplid}, {tuple_data}\n")
        self.hydfs_node.append_file("recoveryJournal.log", journal_filename)
        
        self.resend_and_wait_ack(send_set, task_id)
        
    def recoveryStateProcess(self, task_id):
        global_set = set()
        local_filename = f"{task_id}_journal.log"
        result_dict = {}
        try:
            self.hydfs_node.get_file(local_filename, local_filename)
            
            

            with open(local_filename, "r") as file:
                lines = file.readlines()

            for line in reversed(lines):
                if line.startswith("-100"):
                    continue

                parts = line.strip().split(", ", 1) 
                if len(parts) != 2:
                    continue 
                
                key, value = parts

                if key in global_set:
                    continue 
                
                result_dict[key] = value
                global_set.add(key)


        except Exception as e:
            print(f"Error processing journal file {local_filename}: {e}")

        finally:
            
            self.aggregate_dict = result_dict
            
            if os.path.exists(local_filename):
                os.remove(local_filename)
                print(f"Local file {local_filename} removed.")

    
    def resend_and_wait_ack(self, send_set, task_id):
        
        batch_data = [[] for _ in range(3)]  
        current_time = time.time()
        
        for tuplid, tuple_data in send_set:
     
            group = self.get_hash_num(tuple_data[0])
            batch_data[int(group) - 1].append((tuple_data, tuplid))
        

        with self.ack_lock:
            self.waitingack.extend((tuple_data, tuplid, current_time) for tuple_data, tuplid in batch)
        stage = task_id.split('_')[1]
        if stage == 'Spout':
            for i, batch in enumerate(batch_data):
                ip = self.get_worker_ip(self.topology["Task_Bolt1_"+str(i+1)])
                self.send_line(batch, (ip, STREAMING_PORT_DIC["Task_Bolt1_"+str(i+1)]), task_id)
        elif stage == 'Bolt1':
            for i, batch in enumerate(batch_data):
                ip = self.get_worker_ip(self.topology["Task_Bolt2_"+str(i+1)])
                self.send_line(batch, (ip, STREAMING_PORT_DIC["Task_Bolt2_"+str(i+1)]), task_id)

    
    def cleanup(self):
        return

    
    def execute(self, task_request):
        """
        :param task_id: task ID
        :param operator: task type(Transform, FilteredTransform, AggregateByKey)
        :param data: input stream
        :return: result
        """
        while self.topology == {}:
            print("Waiting for topology update")
            time.sleep(0.2)
    
        print("Topology updated")
        tasktype = self.task_request["type"]
        operator = self.task_request["operator"]
        task_id = self.task_request["task_id"]
        data = self.task_request["data"]
        
        
        if data["order"] == "recover":
            self.recoverProcess(task_id)
        
        # target = self.find_target(task_id, self.task_request["topology"])
        target ="abad"
        source = self.task_request["source"]
        # destination = self.task_request["destination"]
        partition = self.task_request["partition"]
        # order = data['order']
        # self.hydfs_node.get_file(source, HYDFS_LOCAL_DIR+source)
        threading.Thread(target=self.wait_for_ack, args=(task_id,)).start()
        # create hydfs journal file and Log file
        # self.createJournalAndLog(task_id)
        

        if tasktype == "Transform":
            print("Transform")
            stopthread = threading.Event() 
            # self.recoverProcess(task_id)
            t = threading.Thread(target=self._transform, args=(operator, target, task_id, stopthread)).start()

            while True:
                time.sleep(1)
                
            self.threadpool[task_id] = {'thread': t, 'stopthread': stopthread}
            # return self._transform(operator, target, task_id)

        elif tasktype == "FilteredTransform":
            print("FilteredTransform")
            stopthread = threading.Event() 
            # self.recoverProcess(task_id)
            t = threading.Thread(target=self._filtered_transform, args=(operator, target, task_id, stopthread)).start()
            while True:
                time.sleep(1)
            self.threadpool[task_id] = {'thread': t, 'stopthread': stopthread}
            # return self._filtered_transform(operator, target, task_id)
        elif tasktype == "AggregateByKey":
            print("AggregateByKey")
            stopthread = threading.Event() 
            # self.recoveryStateProcess(task_id)
            t = threading.Thread(target=self._aggregate_by_key, args=(operator, target, task_id, stopthread)).start()
            while True:
                time.sleep(1)
            self.threadpool[task_id] = {'thread': t, 'stopthread': stopthread}
            # return self._aggregate_by_key(operator, target, task_id)
        elif tasktype == "Spout":
            print("Spout")
            stopthread = threading.Event() 
            t = threading.Thread(target=self._spout, args=(operator, target, task_id, stopthread, source, partition, 3)).start()
            while True:
                time.sleep(1)
            self.threadpool[task_id] = {'thread': t, 'stopthread': stopthread}
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def get_tupl_id(self, vm,task_id, current):
        return f'{vm}#{task_id}#{current}'
    
    
    def _spout(self, operator, target, task_id, stopthread, input_file, partition, num_of_task=3):

        start = partition[0]
        cuttent = start
        end = partition[1]
        batch_data = [ []for _ in range(num_of_task)]
        counter = 0
        inputfile = input_file
        time.sleep(1)
        self.ack_lock.acquire()
        self.ack_pending_queue.append((task_id, self.ip))
        self.ack_lock.acquire()
        try:
            while not stopthread.is_set():
                with open(HYDFS_LOCAL_DIR+inputfile, "r") as f:
                    for line in islice(f, start - 1, end):
                        # tuple_id = f'{task_id}#{cuttent}'


                        tupl = (input_file +":"+str(cuttent), line.strip())
                        cuttent += 1
                        current_time = time.time()
                        group = self.get_hash_num(tupl[0])
                        batch_data[int(group)-1].append((tupl, 
                                                         self.get_tupl_id(self.topology[task_id], task_id, cuttent))
                                                         ) 
                    
                        for i, batch in enumerate(batch_data):
                            # if len(batch) == 10:
                            
                            self.waitingack.extend((tupl, tuplid, current_time) for tupl, tuplid in batch)
                            self.send_line(batch, (self.get_worker_ip(self.topology["Task_Bolt1_"+str(i+1)]), STREAMING_PORT_DIC["Task_Bolt1_"+str(i+1)]), task_id)
                            self.updateJournal(current_time, batch, task_id)
                            self.ack_lock.release()


        finally:
            self.cleanup()
            pass
     #TOOD: 
    def wait_for_ack(self, task_id):

        ack_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # ack_socket.bind(("0.0.0.0", ACK_PORT))
        timeout = 1
        print("waiting for ack", self.ip, ACK_PORT)
        self.ack_lock.acquire()
        while True:
            ack_socket.listen()
            client_socket, client_address = ack_socket.accept()
            data = client_socket.recv(4096).decode("utf-8")
            if data:
                # print("received ack", data)
                # ast.literal_eval(data)
                print("processed ack", data)
                data = set(json.loads(data)[0])
                current_time = time.time()
                timed_out = []
                # with self.ack_lock:
                
                for i, (tupl, tuplid, timestamp) in enumerate(self.waitingack):
                    if tuplid in data:
                        self.waitingack.remove((tupl, tuplid, timestamp))
                    elif current_time - timestamp > timeout:
                        timed_out.append((tupl, tuplid))
                        self.waitingack[i] = (tupl, tuplid, time.time())
                # self.ack_lock.acquire()
                    # self.waitingack = [
                    #     (tupl, tuplid, timestamp) if (current_time - timestamp <= timeout) else timed_out.append((tupl, tuplid))
                    #     for tupl, tuplid, timestamp in self.waitingack
                    #     if tuplid not in data
                    # ]
                    # self.waitingack = [item for item in self.waitingack if item is not None]

                batch_data = [ []for _ in range(3)]
                for item in timed_out:
                    tupl, tuplid = item
                    group = self.get_hash_num(tupl[0])
                    batch_data[int(group)-1].append((tupl, tuplid))

                stage = task_id.split('_')[1]
                if stage == 'Spoutt':
                    for i, batch in enumerate(batch_data):
                        ip = self.get_worker_ip(self.topology["Task_Bolt1_"+str(i+1)])
                        self.send_line(batch, (ip, STREAMING_PORT_DIC["Task_Bolt1_"+str(i+1)]), task_id)
                elif stage == 'Bolt1t':
                    for i, batch in enumerate(batch_data):
                        ip = self.get_worker_ip(self.topology["Task_Bolt2_"+str(i+1)])
                        self.send_line(batch, (ip, STREAMING_PORT_DIC["Task_Bolt2_"+str(i+1)]), task_id)

                # for i, batch in enumerate(batch_data):
                #     ip = self.get_worker_ip(self.topology["Task_Bolt2_"+str(i+1)])
                #     self.send_line(batch, (ip, STREAMING_PORT_DIC["Task_Bolt2_"+str(i+1)]))

    def send_line(self, batch, client_address, task_id):
        # for timestamp, item in timed_out:
        # target = "Task_Bolt1_"+self.get_hash_num(timed_out[0][0])
        # ip = self.get_worker_ip(self.topology[target])
        MAX_RETRIES = 20
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                # print(f"Attempting to send to {client_address} (Retry {retry_count + 1}/{MAX_RETRIES})")
                # sendingsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as send_socket:
                    send_socket.connect((client_address[0], client_address[1]))
                    send_socket.sendall(json.dumps([batch]).encode("utf-8"))
                pass
                break
            # except:
            except (socket.error, ConnectionRefusedError) as e:
                # print(f"Error sending data to {client_address}: {e}")
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    # print(f"Retrying in {0.5} seconds...")
                    time.sleep(0.1)



    def maintain_history(self, tupl):
        while True:
            time.sleep(2)
            with self.history_lock:
                self.history_tuples.remove_expired()

    def _aggregate_by_key(self, operator, target, task_id, stopthread):
        try:
            count = 0
            recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            recv_socket.bind(("0.0.0.0", STREAMING_PORT_DIC[task_id]))
            recv_socket.listen()
            while not stopthread.is_set():  
                client_socket, client_address = recv_socket.accept()
                while client_socket:             
                    data_batch = client_socket.recv(4096).decode("utf-8") # [[tuple, timestamp], [tuple, timestamp]]
                    data_batch = json.loads(data_batch)[0] #{tuple, tupleid}
                    if data_batch == [[]] or data_batch == "" or not data_batch:
                        print(f"empty {client_address}")
                        break
                    input_batch = []
                    ack_sendback = []
                    for (tupl, tupleid) in data_batch:
                        tupl = tuple(tupl)
                        if tupl in self.history_tuples:
                            continue
                        else:
                            input_batch.append(tupl)
                            ack_sendback.append(tupleid)
                            self.history_tuples.add(tupl, time.time())
                    if input_batch == []:
                        print(f"empty {client_address}")
                        break
                    input_data = "\n".join([f"{json.dumps(tupl)}" for tupl in input_batch])

                    opt_name, opt_args = self.handle_opretor(operator)
                    opt_name = "/home/twei11/ga0/mp4/common/dist/op_nothing"
                    process = subprocess.Popen(
                    [opt_name]+opt_args,                      # 目标程序路径
                    stdin=subprocess.PIPE,         # 输入流
                    stdout=subprocess.PIPE,        # 输出流
                    stderr=subprocess.PIPE,        # 错误流
                    text=True                      # 文本模式
                    )

                    # stdout, stderr = process.communicate(input=input_data)
                    stdout, stderr = process.communicate(input=input_data)
                    processed_results = []
                    # stdout, stderr = process.communicate(input=input_data)
                    for line in stdout.splitlines():
                        processed_results.append(ast.literal_eval(line))

                    print("output: ",stdout, "error", stderr)
                    if processed_results == []:
                        break
                    print("processed_results", type(processed_results[0]))
                    current_time = time.time()

                    # processed_results = [(json.loads(line))for line in stdout.strip().split("\n")] #{tuple}
                    
                    
                    # local_waiting_ack = []
                    send_out_batch = []
                    for (key, value) in processed_results:
                        
                        #tupl id
                        tupleid = self.get_tupl_id(self.topology[task_id], task_id, count)
                        #send ack
                        count+=1
                        # local_waiting_ack.append((tupl, tupleid, current_time))
                        #send out

                        # for key, value in tupl.items():
                        with self.aggregate_lock:
                            self.aggregate_dict[key] = int(self.aggregate_dict.get(key, 0)) + 1
                        tuplenew = (key, self.aggregate_dict[key])
                        send_out_batch.append((tuplenew, tupleid))
                    # update journal
                    self.updateJournal(current_time, send_out_batch, task_id)
                    self.updateLog(current_time, ack_sendback)
                    self.send_line(send_out_batch, (NIMBUS_STREAM_IP, NIMBUS_STREAM_PORT) , task_id)    
                    self.send_ack(ack_sendback, client_address)
        finally:
            self.cleanup()
            pass
    def _filtered_transform(self, operator, target, task_id, stopthread):
        try:
            count = 0
            recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            recv_socket.bind(("0.0.0.0", STREAMING_PORT_DIC[task_id]))
            recv_socket.listen(100000)
            while not stopthread.is_set():  
                client_socket, client_address = recv_socket.accept()
                while client_socket:             
                    data_batch = client_socket.recv(4096).decode("utf-8") # [[tuple, timestamp], [tuple, timestamp]]
                    data_batch = json.loads(data_batch) #{tuple, tupleid}
                    input_batch = []
                    ack_sendback = []
                    for (tupl, tupleid) in data_batch:
                        if tupl in self.history_tuples:
                            continue
                        else:
                            input_batch.append(tupl)
                            ack_sendback.append(tupleid)
                            self.history_tuples.add(tupl, time.time())

                    
                    input_data = "\n".join([f"{json.dumps(tupl)}" for tupl in input_batch])

                    opt_name, opt_args = self.handle_opretor(operator)
                    process = subprocess.Popen(
                    [opt_name]+opt_args,                      # 目标程序路径
                    stdin=subprocess.PIPE,         # 输入流
                    stdout=subprocess.PIPE,        # 输出流
                    stderr=subprocess.PIPE,        # 错误流
                    text=True                      # 文本模式
                    )
                    # stdout, stderr = process.communicate(input=input_data)
                    stdout, stderr = process.communicate(input=input_data)
                    current_time = time.time()
                    processed_results = [(json.loads(line))for line in stdout.strip().split("\n")] #{tuple}
                    
                    
                    local_waiting_ack = []
                    send_out_batch = []
                    for tupl in processed_results:

                        count+=1
                        #tupl id
                        tupleid = self.get_tupl_id(self.topology[task_id], task_id, count)
                        
                        #add ackpending
                        local_waiting_ack.append((tupl, tupleid, current_time))
                        #send out
                        send_out_batch.append((tupl, tupleid))

                    stage = task_id.split('_')[1]

                    if stage == 'Spout':
                        nextstage == 'Bolt1'
                    else :
                        nextstage = 'Bolt2'
                    batch_data = [ []for _ in range(3)]
                    for (tupl, tupleid) in send_out_batch:
                        tasknum = self.get_hash_num(tupl[0])
                        batch_data[int(tasknum)-1].append((tupl, tupleid))
                    self.ack_lock.acquire()
                    self.waitingack.extend(local_waiting_ack)
                    self.send_ack(ack_sendback, client_address)
                    self.updateLog(current_time, ack_sendback)
                    for i, batch in enumerate(batch_data):
                            if batch:
                                # update journal
                                self.updateJournal(current_time, batch, task_id)
                                # update log
                                self.send_line(batch, (self.get_worker_ip(self.topology['Task_'+nextstage+'_'+str(i+1)]), STREAMING_PORT_DIC['Task_'+nextstage+'_'+str(i+1)]) , task_id)     
                            else:
                                pass

        finally:
            self.cleanup()
            pass

    def send_ack(self, batch, client_address):
        sendingsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("sending ack to", client_address[0], ACK_PORT)
        sendingsocket.connect((client_address[0], ACK_PORT))
        sendingsocket.sendall(json.dumps([batch]).encode("utf-8"))



    def handle_opretor(self, operator):
        opt_name = operator.split(':')[0]
        opt_args = operator.split(':')[1:]
        return opt_name, opt_args
        
        pass
    def get_worker_ip(self, worker_id):
        """
        """
        worker_index = int(worker_id.split("#")[0][-1]) - 1
        return HOSTLIST[worker_index]
    def get_hash_num(self, key):
        return (hash(key) % 3)+1
    
    def clean_up(self):
        print("cleaning...")
        pass

    def _transform(self, operator, target, task_id, stopthread):
        try:
            count = 0
            recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            recv_socket.bind( ("0.0.0.0", STREAMING_PORT_DIC[task_id]))
            print("self ip", self.ip,"port", STREAMING_PORT_DIC[task_id])
            recv_socket.listen()
            while not stopthread.is_set():  
                client_socket, client_address = recv_socket.accept()
                while client_socket:   
                    data_batch = client_socket.recv(4096).decode("utf-8") # [[tuple, timestamp], [tuple, timestamp]]
                    if data_batch == "" or not data_batch or data_batch == [[]]:
                        print(f"empty {client_address}")
                        break
                    print("before data_batch", data_batch)
                    data_batch = json.loads(data_batch)[0] #{tuple, tupleid}
                    if data_batch == []:
                        print(f"empty {client_address}")
                        break
                    print("data_batch", data_batch)
                    input_batch = []
                    ack_sendback = []
                    
                    for (tupl, tupleid) in data_batch:
                        tupl = tuple(tupl)
                        if tupl in self.history_tuples:
                            continue
                        else:
                            input_batch.append(tupl)
                            ack_sendback.append(tupleid)
                            self.history_tuples.add(tupl, time.time())

                    if input_batch == []:
                        print(f"empty {client_address}")
                        break
                    input_data = "\n".join([f"{json.dumps(tupl)}" for tupl in input_batch])
                    print("input_data", input_data)
                    opt_name, opt_args = self.handle_opretor(operator)
                    opt_name = "/home/twei11/ga0/mp4/common/dist/op_none"
                    process = subprocess.Popen(
                    [opt_name]+opt_args,                      # 目标程序路径
                    stdin=subprocess.PIPE,         # 输入流
                    stdout=subprocess.PIPE,        # 输出流
                    stderr=subprocess.PIPE,        # 错误流
                    text=True                      # 文本模式
                    )
                    # stdout, stderr = process.communicate(input=input_data)
                    processed_results = []
                    stdout, stderr = process.communicate(input=input_data)
                    for line in stdout.splitlines():
                        processed_results.append(ast.literal_eval(line))

                    print("output: ",stdout, "error", stderr)
                    # time.sleep(10001202)
                    current_time = time.time()
                    # processed_results = [(json.loads(line))for line in stdout.strip().split("\n")] #{tuple}
                    # ast.literal_eval(processed_results)
                    print("processed_results", type(processed_results[0]))
                    
                    local_waiting_ack = []
                    send_out_batch = []
                    for tupl in processed_results:
                        count+=1
                        #tupl id
                        tupleid = self.get_tupl_id(self.topology[task_id], task_id, count)
                        
                        #add ackpending
                        local_waiting_ack.append((tupl, tupleid, current_time))
                        #send out
                        send_out_batch.append((tupl, tupleid))

                    stage = task_id.split('_')[1]

                    if stage == 'Spout':
                        nextstage == 'Bolt1'
                    else :
                        nextstage = 'Bolt2'
                    batch_data = [ []for _ in range(3)]
                    for (tupl, tupleid) in send_out_batch:
                        tasknum = self.get_hash_num(tupl['key'])
                        batch_data[int(tasknum)-1].append((tupl, tupleid))
                    self.ack_lock.acquire()
                    self.waitingack.extend(local_waiting_ack)
                    self.send_ack(ack_sendback, client_address)
                    self.updateLog(current_time, ack_sendback)
                    for i, batch in enumerate(batch_data):
                            if batch:
                                # update journal
                                self.updateJournal(current_time, batch, task_id)
                                # update log
                                self.send_line(batch, (self.get_worker_ip(self.topology['Task_'+nextstage+'_'+str(i+1)]), STREAMING_PORT_DIC['Task_'+nextstage+'_'+str(i+1)]) , task_id)     
                            else:
                                pass

        finally:
            self.cleanup()
            pass