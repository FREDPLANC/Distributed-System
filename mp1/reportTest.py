import socket
import threading
import asyncio
import subprocess
import sys
from datetime import datetime
import os
import time
import generatLog
exit_flag = False
hostlist = ['172.22.157.76', '172.22.159.77', '172.22.95.76', '172.22.157.77'] # '172.22.159.78', '172.22.95.77', '172.22.157.78', '172.22.159.79', '172.22.95.78', '172.22.157.79']

def handle_client(client_socket):
    request = client_socket.recv(1024).decode()

    hostname = socket.gethostname()
    vm_number = int(hostname.split('-')[-1].split('.')[0][1:]) 
    log_filename = f"vm{vm_number}.log" 

    if request.startswith('grep '):
        # pattern = request[len('grep '):].strip()  
        try:
            command = f'{request} {log_filename}'
            result = subprocess.check_output(command, shell=True, text=True)
            
            chunk_size = 4096
            for i in range(0, len(result), chunk_size):

                try:
                    client_socket.sendall(result[i:i + chunk_size].encode())
                except ConnectionResetError:
                    # print("Connection reset by peer, stopping transmission.")
                    break
        except subprocess.CalledProcessError:
            client_socket.sendall(b"No matches found")
    client_socket.close()

async def client_task(hosts, port, grep_pattern):
    results = []
    tasks = []

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
            
            writer.close()
            await writer.wait_closed()
        except asyncio.TimeoutError:
            # print(f"Connection timeout occurred for {host}. Skipping...")
            results.append((host, "Timeout"))
        except Exception as e:
            # print(f"Failed to connect to {host}: {e}")
            results.append((host, "Error"))

    # Run local grep
    hostname = socket.gethostname()
    local_log_filename = f"vm{int(hostname.split('-')[-1].split('.')[0][1:])}.log"
    local_ip = socket.gethostbyname(hostname)  # get server ip
    try:
        command = f'grep {grep_pattern} {local_log_filename}'
        local_result = subprocess.check_output(command, shell=True, text=True)
        results.append((local_ip, local_result))
    except subprocess.CalledProcessError:
        results.append((local_ip, "No matches found"))

    # For each remote host, send the grep query
    for host in hosts:
        task = asyncio.create_task(handle_host(host))
        tasks.append(task)

    await asyncio.gather(*tasks)

    return results

def compare_results(local_result, remote_result, host, vm_number):
    """Compare the results and print detailed differences.""" 
    localmatching_lines = local_result.count('\n')
    remotematching_lines = remote_result.count('\n')
    if remote_result == "No matches found" and local_result == "No matches found":
        print(f"VM{vm_number}: No matches found.")
    else:
        if local_result == remote_result:
            print(f"VM{vm_number}: Results match.")
            print(f"Line count is {localmatching_lines}")
        else:
            print(f"VM{vm_number}: Results differ:")
            print(f"VM{vm_number}: Local Line count is {localmatching_lines}")
            print(f"VM{vm_number}: remote Line count is {remotematching_lines}")
            # print(f"Local result:\n{local_result}")
            # print(f"Remote result:\n{remote_result}")
             # write differ
            if remotematching_lines < 100:
                diff_filename = f"diff_vm{vm_number}.txt"
                with open(diff_filename, 'w') as diff_file:
                    diff_file.write(f"VM{vm_number} ({host}) Results differ:\n")
                    diff_file.write(f"Local Line count: {localmatching_lines}\n")
                    diff_file.write(f"Remote Line count: {remotematching_lines}\n")
                    diff_file.write("\nLocal result:\n")
                    diff_file.write(local_result)
                    diff_file.write("\nRemote result:\n")
                    diff_file.write(remote_result)
async def run_queries_in_intervals(hosts, port, patterns, interval):
    """Run grep queries at regular intervals with different patterns."""
    # make sure folder exist
    
    result_folder = './ga0/test_result/'
    os.makedirs(result_folder, exist_ok=True)

    #path to result file
    result_file = os.path.join(result_folder, 'latency_results.txt')
    
    #save temp logs for tests
    # tempLogs = []
    # generatLog.generate_logs_with_user_string_mixed(2000, "-h \'Mac OS X\'", tempLogs)
    # generatLog.write_logs_to_file(tempLogs, "tempLogfile")

    # run every query 5 times
    for pattern in patterns:
        print("\n" + "=" * 50)
        print(f"Running query for pattern:\n{pattern}")
        
        # record 5 latencies
        latencies = []
        
        for i in range(5):  # run every query 5 times
            start_time = time.time()
            
            # run client_task and collect result
            results = await client_task(hosts, port, pattern)
            
            end_time = time.time()
            latency = end_time - start_time
            latencies.append(latency)  # add latencies
            
            # compare reslut if needed
            for host, remote_result in results:
                vm_number = hostlist.index(host) + 1
                local_log_filename = f"./testlogs/vm{vm_number}.log"
                try:
                    command = f'grep {pattern} {local_log_filename}'
                    local_result = subprocess.check_output(command, shell=True, text=True)
                except subprocess.CalledProcessError:
                    local_result = "No matches found"
                
                compare_results(local_result, remote_result, host, vm_number)
        
        # write all latencies in one line
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        result_file = './ga0/test_result/latency_results.txt'
        with open(result_file, 'a') as f:
            latency_line = ', '.join(f"{lat:.4f}" for lat in latencies)
            f.write(f"{latency_line}\n")
        
        await asyncio.sleep(interval)




def server_task(host, port):
    global exit_flag
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    # print(f"Server listening on {host}:{port}...")
    while not exit_flag:
        try:
            server_socket.settimeout(1)
            client_socket, addr = server_socket.accept()
            # print(f"Accepted connection from {addr}")
            sys.stdout.flush()
            client_handler = threading.Thread(target=handle_client, args=(client_socket,))
            client_handler.start()
        except socket.timeout:
            continue 
        except OSError:
            break
    
    server_socket.close()



async def main():
    global exit_flag
    global hostlist
    port = 20000
    
    hostname = socket.gethostname()
    vm_number = int(hostname.split('-')[-1].split('.')[0][1:])  
    host = hostlist[vm_number - 1] 

    hosts = [ip for i, ip in enumerate(hostlist) if i != (vm_number - 1)]
    
    # Define query patterns and interval in seconds
    query_patterns = [
        # '-h \'"http://vega.com/app/categories/explore/faq/"\'', 
        # '-h \'www.morton.com\'', 
        # '-h \'http://www.munoz.com/categories/search/index.htm" "Mozilla/5.0 (Windows 98; Win 9x 4.90) AppleWebKit/5342 (KHTML, like Gecko) Chrome/14.0.829.0\'', 
        # '-h \'HTTP/1.0" 200 4987 "http://www.conner.com/home/"\''
        '-h \'Mac OS X\'', 
        '-h \'POST /list HTTP/1.0\'', 
        '-h \'Safari\'', 
        '-h \'GET\'',
        '-h \'http\''
        ]
    interval = 3  # Interval between queries in seconds
    
    server_thread = threading.Thread(target=server_task, args=(host, port))
    server_thread.start()

    # Run queries at intervals
    await run_queries_in_intervals(hosts, port, query_patterns, interval)

    # Shut down the server after queries are complete
    exit_flag = True
    # print("Shutting down server...")
    server_thread.join()

if __name__ == "__main__":
    asyncio.run(main())
