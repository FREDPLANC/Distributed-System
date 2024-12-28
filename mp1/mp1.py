import socket
import threading
import asyncio
import subprocess
import sys
from datetime import datetime
import os

# Global flag to control server shutdown
exit_flag = False
# List of host IP addresses (10 VMs in this case)
hostlist = ['172.22.157.76', '172.22.159.77', '172.22.95.76', '172.22.157.77', '172.22.159.78', '172.22.95.77', '172.22.157.78', '172.22.159.79', '172.22.95.78', '172.22.157.79']


# Function to handle incoming client requests
def handle_client(client_socket):
    """
    Handles incoming client requests to execute a `grep` command on the local log file.
    
    Input: client_socket (socket object) - The socket connected to the client.
    Output: Sends the result of the `grep` command to the client.
    """
    request = client_socket.recv(1024).decode()

    # Get the local hostname and VM number to identify the corresponding log file
    hostname = socket.gethostname()
    vm_number = int(hostname.split('-')[-1].split('.')[0][1:]) 
    log_filename = f"vm{vm_number}.log" 

    # If the request is a valid 'grep' command
    if request.startswith('grep '):
        pattern = request[len('grep '):].strip()  
        try:
            command = f'grep {pattern} {log_filename}'
            result = subprocess.check_output(command, shell=True, text=True)
            # command = f'{request} {log_filename}'
            # result = subprocess.check_output(command, shell=True, text=True)
            
            # Send the grep result back to the client in chunks (to avoid large data overflow)
            chunk_size = 4096
            for i in range(0, len(result), chunk_size):
                try:
                    client_socket.sendall(result[i:i + chunk_size].encode())
                except ConnectionResetError:
                    print("Connection reset by peer, stopping transmission.")
                    break
        except subprocess.CalledProcessError:
            client_socket.sendall(b"No matches found")
        except ConnectionResetError:
            print("Connection reset by peer during initial transmission.")
    print("Enter 'grep <pattern>' to search logs or 'exit' to quit: ")
    client_socket.close()

# Asynchronous task to send a grep query to all hosts and handle the results
async def client_task(hosts, port, grep_pattern):
    """
    Sends a `grep` query to all hosts asynchronously and handles their responses.
    
    Input: 
        hosts (list): List of host IP addresses to send the query.
        port (int): Port to communicate with the servers.
        grep_pattern (str): The search pattern for grep command.
    
    Output: Saves each host's result in a timestamped folder.
    """
    results = []
    tasks = []

    # Create a timestamped folder to store the grep results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(timestamp, exist_ok=True)
    
    async def handle_host(host):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            
            writer.write(f'grep {grep_pattern}\n'.encode())
            await writer.drain()
            
            log_data = ''
            while True:
                chunk = await reader.read(4096) 
                if not chunk:
                    break
                log_data += chunk.decode()

            results.append((host, log_data))
            # print("inside results", results)
           
            writer.close()
            await writer.wait_closed()
        except asyncio.TimeoutError:

            print(f"Connection timeout occurred for {host}. Skipping...")
            results.append((host, "Timeout"))
        except Exception as e:
            print(f"Failed to connect to {host}: {e}")
            results.append((host, "Error"))

    # Get the local VM's hostname and IP
    hostname = socket.gethostname()
    local_log_filename = f"vm{int(hostname.split('-')[-1].split('.')[0][1:])}.log"
    local_ip = socket.gethostbyname(hostname)  # 获取主机的 IP 地址
    
    # Perform grep on the local machine's log
    try:
        command = f'grep {grep_pattern} {local_log_filename}'
        local_result = subprocess.check_output(command, shell=True, text=True)
        # local_result = subprocess.check_output(['grep', '-nH', grep_pattern, local_log_filename]).decode()
        results.append((local_ip, local_result))
    except subprocess.CalledProcessError:
        results.append((local_ip, "No matches found"))

    # Asynchronously send the query to all other hosts
    for host in hosts:
        # print("outsidehost: ", host)
        task = asyncio.create_task(handle_host(host))
        tasks.append(task)

    await asyncio.gather(*tasks)
    totalCount = 0 
    for host, log_data in results:
        vm_number = hostlist.index(host) + 1
        # print("vm:", vm_number)
        if log_data != "Timeout" and log_data != "Error":
            # print(f"Results from {host}:\n{log_data}")
            log_filename = os.path.join(timestamp, f"vm{vm_number}.log")
            
            with open(log_filename, 'w') as log_file:
                log_file.write(log_data)
            matching_lines = log_data.count('\n')
            totalCount += matching_lines
            print(f"VM{vm_number}:{matching_lines} matching lines from {host}")
            
        else:
            print(f"VM{vm_number}: Failed to retrieve logs {log_data}")
    
    print(f"Summary: the total line count is: {totalCount}")
    # print(results)
    
# Task to start the server and listen for incoming requests
def server_task(host, port):
    """
    Starts the server and listens for incoming `grep` requests from other clients.
    
    Input: 
        host (str): The local IP address of the machine.
        port (int): The port to listen for incoming connections.
    
    Output: Handles client connections and executes queries.
    """
    global exit_flag
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Server listening on {host}:{port}...")
    while not exit_flag:
        try:
            server_socket.settimeout(1)  
            client_socket, addr = server_socket.accept()
            print(f"Accepted connection from {addr}")
            sys.stdout.flush()  
            client_handler = threading.Thread(target=handle_client, args=(client_socket,))
            client_handler.start() # Handle the client in a separate thread
        except socket.timeout:
            continue 
        except OSError:
            break
    
    server_socket.close()

# Main function to start the server and handle user input
async def main():
    global exit_flag
    
    global hostlist
    port = 20000
    
    hostname = socket.gethostname()
    vm_number = int(hostname.split('-')[-1].split('.')[0][1:])  
    host = hostlist[vm_number - 1] 

    hosts = [ip for i, ip in enumerate(hostlist) if i != (vm_number - 1)]

   
    server_thread = threading.Thread(target=server_task, args=(host, port))
    server_thread.start()
    
    # Loop to handle user input
    while True:
        sys.stdout.write("Enter 'grep <pattern>' to search logs or 'exit' to quit: ")
        sys.stdout.flush()  
        command = input()  
        if command.startswith('grep '):
            grep_pattern = command[len('grep '):]
            await client_task(hosts, port, grep_pattern)
        elif command == 'exit':
      
            exit_flag = True
            print("Shutting down server...")
            server_thread.join() 
            break

# Run the main function when the script is executed
if __name__ == "__main__":
    asyncio.run(main())