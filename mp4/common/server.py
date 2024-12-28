import socket
import subprocess
import signal
import sys
import time
import os
import random
import threading
# from faker import Faker
from membership_node import Node, getvmnum ,Introducer
# import psutil


log_levels = ['INFO', 'WARNING', 'ERROR', 'DEBUG']

# import psutil
import time

# def get_network_bandwidth(interface="ens33", interval=1):

#     net_io_before = psutil.net_io_counters(pernic=True)[interface]
#     bytes_sent_before = net_io_before.bytes_sent
#     bytes_recv_before = net_io_before.bytes_recv


#     time.sleep(interval)

#     net_io_after = psutil.net_io_counters(pernic=True)[interface]
#     bytes_sent_after = net_io_after.bytes_sent
#     bytes_recv_after = net_io_after.bytes_recv


#     sent_bandwidth = (bytes_sent_after - bytes_sent_before) / interval
#     recv_bandwidth = (bytes_recv_after - bytes_recv_before) / interval

#     return sent_bandwidth, recv_bandwidth

# def record_band():
#     interface = "ens33"  
#     while True:
#         sent_bw, recv_bw = get_network_bandwidth(interface)
#         print(f"Sent: {sent_bw / 1024:.2f} KB/s, Received: {recv_bw / 1024:.2f} KB/s")

def handle_query(connection):
    try:
        # Process the client
        query = connection.recv(1024).decode('utf-8')
        print("Client connected, processing query " + query)
        
        query_array = query.split(" ")
        
        #Handle a create command (sent if --populate flag is enabled) that will create the randomized log files for the unit tests
        if(query_array[0] == "create"):
            create_log_file(query_array[1])
            connection.sendall("machine."+query_array[1]+".log is ready to be queried")
        else:
            # Execute grep command locally
            result = subprocess.run(query, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(result.stdout)
            print(result.stderr)
            #Send back the results of the command
            connection.sendall(result.stdout)
            connection.sendall(result.stderr)   

    except Exception as e:
        connection.sendall(str(e).encode('utf-8'))
    finally:
        connection.close()






def start_server(port):  
    #Standard Socket Startup
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)
    server_socket.setblocking(False)  # Make the server socket non-blocking
    print(f"Server started on port {port}")

    #Close on CTRL + C

    #Run Server
    while True:
        try:
            try:
                client_socket, addr = server_socket.accept()
                handle_query(client_socket)
            except BlockingIOError:
                # No connection available, continue the loop
                time.sleep(0.1)  # Briefly sleep to avoid busy-waiting

        except Exception as e:
            print(f"Error handling client: {e}")

if __name__ == "__main__":
    # start_server(9999)
    hostlist = ['172.22.157.76', '172.22.159.77', '172.22.95.76', 
            '172.22.157.77', '172.22.159.78', '172.22.95.77', 
            '172.22.157.78', '172.22.159.79', '172.22.95.78', 
            '172.22.157.79'] #1 - 10
    
    machines = [{'ip': ip, 'port': 9999} for ip in hostlist]

    host = hostlist[int(getvmnum()) - 1] 
    # host+=":9999"
    # node = Node(host)

    if len(sys.argv) < 3:
        print("Usage: python node.py <IP> <Port> [IntroducerIP:IntroducerPort] [DetectionMethod]")
        sys.exit(1)
    node_ip = sys.argv[1]
    node_port = sys.argv[2]
    # node_port = 9999
    # introducer_ip = sys.argv[3] if sys.argv[3] != 'None' else None
    # introducer_ip = '172.22.157.76:9999'

    introducer_ip = sys.argv[3] if sys.argv[3] != 'None' else None
    detection_method = sys.argv[4]
    port2 = 8848
    # threading.Thread(target=start_server, args=(port2, )).start()
    # threading.Thread(target=record_band).start()
    print(introducer_ip)
    # if introducer_ip != host+':9999':
    if introducer_ip != None:
        node = Node(node_ip, int(node_port), introducer_ip, 'PingAck', 0) 
    else:
        node = Introducer(node_ip, node_port, 'PingAck', 0)
    while node.is_running:
        time.sleep(0.1)

    # print(host)

#vm1: python server.py 172.22.157.76 9999 None PingAck
#vm2: python server.py 172.22.159.77 9999 172.22.157.76:9999 PingAck
#vm2: python server.py 172.22.95.76 9999 172.22.157.76:9999 PingAck