import socket
import subprocess
import signal
import sys
import time
import os
import random
import threading
# from faker import Faker
import logging

class StopExecution(Exception):
    pass
def getvmnum():
    hostname = socket.gethostname()
    return str(hostname.split('-')[-1].split('.')[0][1:])

class Node:
    def __init__(self, node_ip, node_port, introducer_ip=None,  detection_method = 'PingAck', drop_rate = 0):
        self.vm_num = getvmnum()
        self.ip = node_ip
        self.port = node_port
        self.incarnation = int(time.time() * 1000)
        self.node_id = f"{self.ip}:{self.port}#{self.incarnation}"  # Unique identifier, e.g., '172.22.157.76:9999#incarnation'
        # self.node_id+=self.vm_num
        print("Creating node: ", self.node_id)
        #mark version
        self.introducer_ip = introducer_ip
        self.timestamp = int(time.time() * 1000)
        self.membership_list = {}  # old: {node_id: {'status': 'Alive/Suspect/Failed', 'timestamp': time}} 
        # {node_id: {'ip': ip, 'port': port, 'incarnation': inc, 'status': status, 'timestamp': timestamp}}
        self.lock = threading.Lock() #
        self.modelock = threading.Lock()
        self.encode_lock = threading.Lock() 
        self.pending_pings_lock = threading.Lock()
        self.is_running = True 
        self.detection_method = detection_method
        self.sequence_number = 0
        self.pending_pings = {} # {(target_node_id, seq_num): timestamp}
        self.pending_sus_mesg = []
        # UDP socket for failure detection
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind((self.ip, int(self.port)))
        self.drop_rate = drop_rate
        # TCP socket for client queries (if needed)
        # self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.tcp_sock.bind((self.get_ip(), self.get_tcp_port()))
        # self.tcp_sock.listen(5)
        # self.tcp_sock.setblocking(False)
        
        #logging
        self.exit_event = threading.Event()
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        fh = logging.FileHandler(f'mp2.log')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        logger.addHandler(ch)
        logger.addHandler(fh)
        self.logger = logger
        self.thread_pool = []
        # Start threads
        messages_thread = threading.Thread(target=self.listen_for_udp_messages, daemon=True)
        self.thread_pool.append(messages_thread)
        messages_thread.start()
        
        failure_thread = threading.Thread(target=self.failure_detector, daemon=True)
        self.thread_pool.append(failure_thread)
        failure_thread.start()

        command_thread = threading.Thread(target=self.handle_command, daemon=True)
        self.thread_pool.append(command_thread)
        command_thread.start()
        #we have 3 thread, one for udp message, one for failure detect, one for client queries(e.g. grep)

        if self.introducer_ip:
            self.join_group()
        else:
            print("missing introducer, I AM THE INTRODUCER!(if correct)")

    def get_ip(self):
        return self.node_id.split(':')[0]

    def get_tcp_port(self):
        return int(self.node_id.split(':')[1])+ 1
    def send_message_to(self, message, address):
        if random.random() > self.drop_rate:
            # self.send_message_to(message=message, address= address)
            self.udp_sock.sendto(message.encode('utf-8'), (address))
        else:
            logging.info(f"mocking send to {address} failed")

    def get_udp_port(self):
        return int(self.node_id.split(':')[1])   # Offset for UDP port. tcp +1
    
    def listen_for_udp_messages(self):
        while self.is_running:
            try:
                data, addr = self.udp_sock.recvfrom(40960)
                message = data.decode('utf-8')
                # print("recieved message", message)
                self.handle_udp_message(message, addr)
            except Exception as e:
                print(f"error in udp：{e}")

    def handle_udp_message(self, message, addr):
        parts = message.strip().split() 
        command = parts[0]
        if command == 'PING':
            #TODO:
            if parts[4] == self.node_id:
                sender_id = parts[1]
                seq_num = parts[2]
                member_list = parts[3]
                ack_message = f'ACK {self.node_id} {seq_num}'
                self.send_message_to(ack_message, addr)
                # self.udp_sock.sendto(ack_message.encode('utf-8'), addr)
                member_list = self.decode_membership_list(parts[3])
                self.update_member_table_status(member_list)
            else:
                pass
            
            # pass
        elif command == 'ACK':
            #TODO:
            sender_id = parts[1]
            seq_num = parts[2]
            # self.update_member_status(sender_id, 'Alive', time.time(),)
            self.handle_ack(sender_id, int(seq_num))
            # pass
        elif command == 'SUSPECT':
            suspected_node_id = parts[1]
            incarnation = int(parts[2])
            self.handle_suspect(suspected_node_id, incarnation)
            #TODO:
            pass
        # elif command == 'FAIL':
        #     failed_node_id = parts[1]
        #     incarnation = int(parts[2])
        #     self.handle_fail(failed_node_id, int(time.time()))
            #TODO:
            
        elif command == 'JOIN':
            new_node_id = parts[1]
            self.handle_new_join(parts, addr)
            #TODO:
            pass
        elif command == 'LEAVE':
            print("leaving")
            leaving_node_id = parts[1]
            # incarnation = int(parts[2])
            self.handle_leave(leaving_node_id, 9999999999)
            #TODO:
            pass
        elif command == 'UPDATE_MEMBERSHIP':
            #TODO:
            self.handle_membership_message(parts)
            pass
        elif message == 'GET_MEMBERSHIP':
            #TODO:
            self.send_membership(addr)
            #for client request
            pass
        elif command == 'PRINT':
            self.print_mem_list()
            #TODO:

            pass
        # elif command == 'PING-REQUEST':# message: "PING-REQUEST {self.node_id} {suspected_node_id}"
        #     requester_id = parts[1]
        #     target_node_id = parts[2] 
        #     self.handle_ping_request(requester_id, target_node_id)
        # elif command == 'INDIRECT-ACK':
        #     sender_id = parts[1]
        #     target_node_id = parts[2]
        #     status = parts[3]
        #     self.handle_indirect_ack(target_node_id, status)
        else:
            print("Unknown command")

    def change_detection_method(self, method):
        with self.lock:
            self.detection_method = method

    def failure_detector(self):
        while self.is_running:
            time.sleep(0.5)#detect freq 1s
            self.perform_pingack()
            # with self.lock:
            #     if self.detection_method == 'PingAck':

            #         #TODO:
            #         pass
            #     elif self.detection_method == 'PingAck+S':
            #         #TODO:
            #         pass

    def perform_pingack(self):
        with self.lock:
            alive_nodes = [node_id for node_id, member in self.membership_list.items() if member['status'] == 'Alive' and node_id != self.node_id]
        if alive_nodes:
            # target_node_id = random.choice(alive_nodes,min(len(alive_nodes), 3))
            target_nodes = random.sample(alive_nodes, min(len(alive_nodes), 3))
            for id in target_nodes:
                self.send_ping(id)

    def print_mem_list(self):
        self.logger.info("----------------------------")
        for node_id, data in self.membership_list.items():
                    self.logger.info("status: %s, ID: %s", data['status'],node_id)
        self.logger.info("----------------------------")
        self.leave_group()

    def handle_command(self):
        while self.is_running:
            command = input()
            if command.strip() == 'enable_sus':
                self.enable_sus()
            elif command.strip() == 'disable_sus':
                self.disable_sus()
            elif command.strip() == 'leave':
                self.leave_group()
                self.exit_event.set()
                break
            elif command.strip() == 'list_mem':
                for node_id, data in self.membership_list.items():
                    self.logger.info("status: %s, ID: %s", data['status'],node_id)
            elif command.strip() == 'list_self':
                msg = "Self ID:" + self.node_id
                self.logger.debug(msg)
            elif command.strip() == 'grep':
                continue
            elif command.strip() == 'status_sus':
                if self.detection_method == "PingAck+S":
                    print("PingAck+S is on")
                else:
                    print("PingAck+S is off")
            elif command.strip() == "pending":
                print(self.pending_pings)
            elif command.strip() == 'print':
                self.broadcast_udp_message("PRINT")
                self.print_mem_list()
        #TODO:
        pass
    def enable_sus(self):
        if self.detection_method == 'PingAck':
            self.modelock.acquire()
            self.detection_method = 'PingAck+S'
            self.modelock.release()
            self.update_member_status("0.0.0.0:0000", 'PingAck+S', int(time.time()))

    def disable_sus(self):
        if self.detection_method == 'PingAck+S':
            self.modelock.acquire()
            self.detection_method = 'PingAck'
            self.modelock.release()
            self.update_member_status("0.0.0.0:0000", 'PingAck', int(time.time()))

        pass
    def broadcast_changemode_message(self, message):
        print("broadcasting changemode")

        with self.lock:
        #TODO:
            print("in broadcasting")
            for node_id in self.membership_list.keys():
                    if node_id != self.node_id:
                        node_ip, node_port, incarnation = self.parse_node_id(node_id)
                        print("broad casting")
                        self.send_message_to(message, (node_ip, node_port))
                        print("finish casting")
    
    def send_membership(self, addr):
        membership_info = self.encode_membership_list()
        message = f'MEMBERSHIP {membership_info}'
        self.send_message_to(message, (addr[0], addr[1]))
        # self.udp_sock.sendto(message.encode('utf-8'), (addr[0], addr[1]))

    def handle_membership_message(self, parts):#update memberships
        data = ' '.join(parts[1:])
        entries = data.strip().split(';')
        with self.lock:
            for entry in entries:
                if not entry:
                    continue
                node_id, ip, port, incarnation, status, timestamp = entry.split(',')

                member = self.membership_list.get(node_id)
                if member is None or int(incarnation) > member['incarnation']:#have latest status
                    self.membership_list[node_id] = {
                        'ip': ip,
                        'port': int(port),
                        'incarnation': int(incarnation),
                        'status': status,
                        'timestamp': float(timestamp)
                    }
                    #logging updating node to status
    def broadcast_udp_message(self, message):
        print("broadcasting")
        with self.lock:
        #TODO:
            print("in broadcasting")
            for node_id in self.membership_list.keys():
                    if node_id != self.node_id and node_id != "0.0.0.0:0000":
                        node_ip, node_port, incarnation = self.parse_node_id(node_id)
                        print("broad casting")
                        # self.sendto()
                        # self.send_message_to(message, (node_ip, node_port))
                        self.udp_sock.sendto(message.encode('utf-8'), (node_ip, node_port))
                        print("finish casting")

            # pass

    def update_member_table_status(self, member_table):
        self.lock.acquire()
        for node_id, member in member_table.items():
            self_member = self.membership_list.get(node_id)
            if self_member:
                if member['timestamp'] > self_member['timestamp'] and member['status']!='Suspect':
                    self.membership_list[node_id] = member
            
            else:
                self.membership_list[node_id] = member
        self.lock.release()
        self.modelock.acquire()
        self.detection_method = self.membership_list["0.0.0.0:0000"]['status']
        self.modelock.release()

    def update_member_status(self, node_id, status, timestamp):

        with self.lock:
            member = self.membership_list.get(node_id)
            if member:
                if timestamp is None or member['timestamp'] <= timestamp:#if newer
                    member['status'] = status
                    member['timestamp'] = int(time.time())
                    if timestamp:
                        pass
                        # member['incarnation'] = incarnation
                    # logging.info(f"Node: {node_id} update to: {status}")
            else:
                # not in member list
                ip, port, inc = self.parse_node_id(node_id)
                self.membership_list[node_id] = {
                    'ip': ip,
                    'port': port,
                    'incarnation': inc,
                    'status': status,
                    'timestamp': int(time.time())
                }
                # logging.info(f"adding {node_id}, status: {status}")

    def join_group(self):
        #TODO:
        # if introducer_udp_port:
        introducer_udp_port = int(self.introducer_ip.split(':')[1]) 
        join_message = f'JOIN {self.node_id}'
        # self.send_message_to(join_message, (self.introducer_ip.split(':')[0], introducer_udp_port))
        self.udp_sock.sendto(join_message.encode('utf-8'), (self.introducer_ip.split(':')[0], introducer_udp_port))

    def leave_group(self):
        #TODO:
        
        time.sleep(1)
        leave_message = f'LEAVE {self.node_id}'
        self.broadcast_udp_message(leave_message)
        self.is_running = False
        self.udp_sock.close()
        # self.tcp_sock.close()
        pass
    # def handle_new_join(self):
    #     #should not been called in normal code. Only introducer handle this.
    #     print("wrong place")
    #     # with self.lock:
    #     #     if new_node_id not in self.membership_list:
    #     #         self.membership_list[new_node_id] = {'status': 'Alive', 'timestamp': time.time()}
    #     #         self.broadcast_udp_message(f'JOIN {new_node_id}')

    #     pass
    def handle_leave(self, leaving_node_id, timestamp=9999999999):
        msg = leaving_node_id + "is leaving"
        self.logger.info(msg)
        self.update_member_status(leaving_node_id, 'Left', 9999999999)

    def send_ping(self, target_node_id):
        # with self.lock:
        self.lock.acquire()
        self.sequence_number += 1
        seq_num = self.sequence_number
        target_ip, target_port, _  = self.parse_node_id(target_node_id)
        member_list = self.encode_membership_list()
        self.lock.release()
        # target_udp_port = int(target_node_id.split(':')[1]) 
        self.update_member_status(self.node_id, "Alive", int(time.time()))
        ping_message = f'PING {self.node_id} {seq_num} {member_list} {target_node_id}'
        self.send_message_to(ping_message, (target_ip, target_port))
        # self.udp_sock.sendto(ping_message.encode('utf-8'), (target_ip, target_port))
        self.lock.acquire()
        self.pending_pings[(target_node_id, seq_num)] = int(time.time()) #{(target_node_id, seq_num): "time_stemp"}
        self.lock.release()
        threading.Thread(target=self.wait_for_ack, args=(target_node_id, seq_num), daemon=True).start()
    
    def parse_node_id(self, node_id):
        ip_port, inc = node_id.split('#')
        ip, port = ip_port.split(':')
        return ip, int(port), int(inc)

   
    def handle_ack(self, sender_id, seq_num):
        with self.pending_pings_lock:
            if (sender_id, seq_num) in self.pending_pings:
                # self.membership_list[sender_id]['status'] = 'Alive'
                # self.membership_list[sender_id]['timestamp'] = time.time()
                self.pending_pings.pop((sender_id, seq_num), None)
                self.update_member_status(sender_id, 'Alive', int(time.time()))

    def handle_suspect(self, suspected_node_id, incarnation):
        # with self.lock:
        self.update_member_status(suspected_node_id, 'Suspect', incarnation)


    def start_suspicion(self, target_node_id):
        self.lock.acquire()
        
        member = self.membership_list.get(target_node_id)
        if member and member['status'] == 'Alive':
            print(target_node_id, " changing to suspect")
            msg = target_node_id + " changing to suspect"
            self.logger.info(msg)
            self.lock.release()
            self.update_member_status(target_node_id, 'Suspect', int(time.time()))


            threading.Thread(target=self.suspicion_timeout, args=(target_node_id, member['timestamp']), daemon=True).start()
        else:
            self.lock.release()

    def suspicion_timeout(self, target_node_id, incarnation):
        timeout = 2  # suspicion timeout
        time.sleep(timeout)
        self.lock.acquire()
        member = self.membership_list.get(target_node_id)
        if member and member['status'] == 'Suspect':
                # time out
            self.lock.release()
            msg = target_node_id + " Failed!"
            self.logger.info(msg)
            self.mark_node_as_failed(target_node_id)
        else:
            self.lock.release()

    def handle_fail(self, failed_node_id, timestamp):
        self.update_member_status(failed_node_id, 'Failed', timestamp)

    def mark_node_as_failed(self, target_node_id):
        # with self.lock:
        self.lock.acquire()
        member = self.membership_list.get(target_node_id)
        if member and member['status'] != 'Failed':
            member['status'] = 'Failed'
            member['timestamp'] = int(time.time())-2
            # logging.info(f"Node{target_node_id} fail")
            self.lock.release()
            # self.broadcast_failure(target_node_id, member['incarnation'])
        else:
            self.lock.release()

    def broadcast_failure(self, node_id, incarnation):
        message = f'FAIL {node_id} {incarnation}'
        self.broadcast_udp_message(message)

    def wait_for_ack(self, target_node_id, seq_num, time_out = 2):
        timeout = time_out 
        start_time = time.time()
        # ack_received = False
        while time.time() - start_time < timeout:
            # with self.lock:
            #     if self.membership_list.get(target_node_id, {}).get('status') == 'Alive':
            #         ack_received = True
            #         break
            if ((target_node_id, seq_num) not in self.pending_pings) or (self.membership_list[target_node_id]['timestamp'] > int(start_time) and self.membership_list[target_node_id]['status'] == 'Alive'):
                return
            # time.sleep(0.1)
            # recieved ACK aand delet
        #not recieve:
        msg = target_node_id + "time out!"
        self.logger.debug(msg)

        if self.detection_method == 'PingAck':
            msg = target_node_id + " Failed! "
            self.logger.info(msg)
            self.mark_node_as_failed(target_node_id)
            # self.pending_pings.pop((target_node_id, seq_num), None)
        elif self.detection_method == 'PingAck+S':
            self.start_suspicion(target_node_id)
            # self.handle_suspect(target_node_id)
            # self.send_ping_request(target_node_id)
        # if not ack_received:
        #     self.handle_suspect(target_node_id)
    def encode_membership_list(self):
        # self.logger.debug("here1")
        with self.encode_lock:
            # self.logger.debug("here")
            new_members = []
            for node_id, member in self.membership_list.items():
                new_member = f"{node_id},{member['ip']},{member['port']},{member['incarnation']},{member['status']},{member['timestamp']}"
                new_members.append(new_member)
            return ';'.join(new_members)
    def decode_membership_list(self, data):
        membership_list = {}
        entries = data.split(';')
        for entry in entries:
            node_id, node_ip, port, inc ,status, timestamp = entry.split(',')
            # membership_list[node_id] = {'status': status, 'timestamp': float(timestamp)}
            membership_list[node_id] = {'ip': node_ip,'port': port, 'incarnation': inc,'status': status, 'timestamp': int(timestamp)}
            
            # self.membership_list[node_id] = {'status': status, 'timestamp': float(timestamp)}
        return membership_list


class Introducer(Node):
    def __init__(self, node_ip, node_port, detection_method='PingAck', drop_rate = 0):
        dr = drop_rate
        super().__init__(node_ip, node_port, detection_method=detection_method, drop_rate=dr)
        part = ["0000","0000"]
        self.membership_list["0.0.0.0:0000"] = {
                'ip': '0000',
                'port': '0000',
                'incarnation': '0000',
                'status': detection_method,
                'timestamp': int(time.time())
            }
        self.join_myself()
        print(f"start introducer, ID: {self.node_id}")
    def handle_new_join(self, parts, addr):
        new_node_id = parts[1]
        print("New join")
        # with self.lock:
        self.lock.acquire()
        if new_node_id not in self.membership_list:
            print("trying add", new_node_id)
            ip, port, inc = self.parse_node_id(new_node_id)
            self.membership_list[new_node_id] = {
                'ip': ip,
                'port': port,
                'incarnation': inc,
                'status': 'Alive',
                'timestamp': int(time.time())
            }
            # self.lock.release()
            msg = "New node: "+new_node_id+"join"
            self.logger.info(msg)

        self.lock.release()


    def join_myself(self):
        if self.node_id not in self.membership_list:
            print("add introducer", self.node_id)
            ip, port, inc = self.parse_node_id(self.node_id)
            self.lock.acquire()
            self.membership_list[self.node_id] = {
                    'ip': ip,
                    'port': port,
                    'incarnation': inc,
                    'status': 'Alive',
                    'timestamp': int(time.time())
                }
            self.lock.release()
