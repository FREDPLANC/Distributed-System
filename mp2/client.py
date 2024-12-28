import socket
import argparse

#Custom function for assertions for unit tests
# def test_assert(condition, passed, fail):
#     try:
#         assert condition
#         print(f"\033[1;92m{passed}\033[0m")  # Bright green text
#         return True
#     except AssertionError:
#         print(f"\033[91m{fail}\033[0m")  # Red text for failure
#         return False

#Function that sends a query that generates logs for the unit tests on all machines
# def create_logs(machines):
#     results = {}
#     index = 1
    
#     for machine in machines:
        
#         request = "create " + str(index)
        
#         ip, port = machine['ip'], machine['port']
#         result = query_machine(ip, port, request)
#         results[ip] = result
        
#         index += 1
        
#     return results

#Function to receive the server's response to the query in 1 MB chunks. Smaller sizes decrease query speed.
def receive_data_in_chunks(sock, buffer_size=1024*1024):
    data = b''
    while True:
        chunk = sock.recv(buffer_size)
        if not chunk:
            break
        data += chunk
    return data.decode('utf-8')

#Function to send a query to a specific machine
def query_machine(ip, port, query):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip, port))
        client_socket.sendall(query.encode('utf-8'))
        
        response = receive_data_in_chunks(client_socket)
        return response
    except Exception as e:
        return f"Error querying {ip}: {str(e)}"
    finally:
        client_socket.close()

#Function that calls query_machine on every machine and returns the results in a dictionary
#with the IP addresses as keys and the results as values.
def grep_all_machines(machines, query):
    results = {}
    index = 1
    for machine in machines:
        
        #Queries the corresponding log file
        grep_request = query +" mp2.log"
        
        ip, port = machine['ip'], machine['port']
        result = query_machine(ip, port, grep_request)
        results[ip] = result
        
        index += 1
        
    return results

#Print results of a grep query. The line counts are outputted to the terminal. The actual results are written to .txt files
def print_results(results):
    index = 1
    cumulative = 0
    for ip, result in results.items():
        print(f"Results from Machine {index} at IP {ip}:")
        # print(result)
        
        with open(f'Machine_{index}_results.txt', 'w') as file:
            file.write(result)
        
        print("Amount of lines pattern appeared : " + str(result.count('\n')) + "\n")
        cumulative += result.count('\n')
        
        
        if "Errno" in result:
            print("Machine " + str(index) +  " was faulty and/or not responsive\n")
        
        index += 1
    
    print(f"Amount of lines pattern appeared accross all machines : {cumulative}")
    
    return cumulative

#The following lines are unit tests. The unit tests individually check the response of every single responsive machine
#and also checks their cumulative results based on how many machines respond. It returns the result of the test.
    
#Unit test on a pattern that exists frequently (15000 times) on every machine.    
def test_frequent(machines):
    results = grep_all_machines(machines, "grep Mchine")
    index = 1
    cumulative = 0
    respondents = 0
    
    for ip, result in results.items():
        print(f"Results from Machine {index} at IP {ip}:")
        print(result)
        grep_count = result.count('\n')
        print(f"Amount of lines pattern appeared : {grep_count}\n")
        cumulative += grep_count
        
        index += 1
        if "Errno" not in result:
            respondents +=1
            if not test_assert(grep_count == 15000, "Correct Line Count", "Incorrect Line Count"):
                return False
    
    print(f"Amount of lines pattern appeared accross all machines : {cumulative}")
    return test_assert(cumulative == respondents * 15000, "Correct Overall Line Count", "Incorrect Overall Line Count")
    
    
#Unit test on a pattern that exists once on every machine    
def test_rare(machines):
    results = grep_all_machines(machines, "grep Rre")
    
    index = 1
    cumulative = 0
    respondents = 0
    
    for ip, result in results.items():
        print(f"Results from Machine {index} at IP {ip}:")
        print(result)
        grep_count = result.count('\n')
        print(f"Amount of lines pattern appeared : {grep_count}\n")
        cumulative += grep_count
        
        index += 1
        if "Errno" not in result:
            respondents +=1
            if not test_assert(grep_count == 1, "Correct Line Count", "Incorrect Line Count"):
                return False
    
    print(f"Amount of lines pattern appeared accross all machines : {cumulative}")
    return test_assert(cumulative == respondents, "Correct Overall Line Count", "Incorrect Overall Line Count")
    
    
#Unit test on a pattern that exists 5000 times on every machine.    
def test_somewhat(machines):
    results = grep_all_machines(machines, "grep rpeated")
    index = 1
    cumulative = 0
    respondents = 0
    
    for ip, result in results.items():
        print(f"Results from Machine {index} at IP {ip}:")
        print(result)
        grep_count = result.count('\n')
        print(f"Amount of lines pattern appeared : {grep_count}\n")
        cumulative += grep_count
        
        index += 1
        if "Errno" not in result:
            respondents +=1
            if not test_assert(grep_count == 5000, "Correct Line Count", "Incorrect Line Count"):
                return False
        else:
            print("This machine was not responsive")
    
    print(f"Amount of lines pattern appeared accross all machines : {cumulative}")
    return test_assert(cumulative == respondents * 5000, "Correct Overall Line Count", "Incorrect Overall Line Count")

#Unit test on a pattern that doesn't exist on any machine.
def test_none(machines):
    results = grep_all_machines(machines, "grep agilang2")
    
    index = 1
    cumulative = 0
    respondents = 0
    
    for ip, result in results.items():
        print(f"Results from Machine {index} at IP {ip}:")
        print(result)
        grep_count = result.count('\n')
        print(f"Amount of lines pattern appeared : {grep_count}\n")
        cumulative += grep_count
        
        index += 1
        if "Errno" not in result:
            respondents +=1
            if not test_assert(grep_count == 0, "Correct Line Count", "Incorrect Line Count"):
                return False
    
    print(f"Amount of lines pattern appeared accross all machines : {cumulative}")
    return test_assert(cumulative == 0, "Correct Overall Line Count", "Incorrect Overall Line Count")

#Unit test on a pattern that should only exist on Machine 2    
def test_one(machines):
    results = grep_all_machines(machines, "grep 'Mchine 2'")
    
    index = 1
    cumulative = 0
    should_have = 0
    
    for ip, result in results.items():
        print(f"Results from Machine {index} at IP {ip}:")
        print(result)
        grep_count = result.count('\n')
        print(f"Amount of lines pattern appeared : {grep_count}\n")
        cumulative += grep_count
        
        
        if "Errno" not in result:
            if(grep_count != 0):
                should_have = index
            if not test_assert(( grep_count == 15000 and index == 2) or (grep_count == 0 and index != 2), "Correct Machine has the pattern", "Incorrect Machine has pattern"):
                return False
            
        index += 1
    
    print(f"Amount of lines pattern appeared accross all machines : {cumulative}")
    return test_assert(should_have  == 2, "Pattern only found on the correct Machine (Machine 2)", "Pattern should not have been found on more than one Machine")
  
#Unit test for a pattern that exists on every odd numbered machine    
def test_some(machines):
    results = grep_all_machines(machines, "grep Hrein")
    
    index = 1
    cumulative = 0
    have = 0
    should_have = 0
    
    for ip, result in results.items():
        print(f"Results from Machine {index} at IP {ip}:")
        print(result)
        grep_count = result.count('\n')
        print(f"Amount of lines pattern appeared : {grep_count}\n")
        cumulative += grep_count
        
        if "Errno" not in result:
            if(index  % 2  == 1 ):
                should_have += 1
            
            if(grep_count != 0):
                have += 1
            if not test_assert(( grep_count == 10000 and index % 2 == 1) or (grep_count == 0 and index %2 == 0), "Correct Line Count", "Incorrect Line Count"):
                return False
            
        index += 1
    
    print(f"Amount of lines pattern appeared accross all machines : {cumulative}")
    return test_assert(have  ==  should_have, "Pattern found on the correct Machines", "Pattern was not found on the correct Machines")

#Unit test for a pattern that exists on every machine
def test_all(machines):
    results = grep_all_machines(machines, "grep pttern")
    index = 1
    cumulative = 0
    respondents = 0
    
    for ip, result in results.items():
        print(f"Results from Machine {index} at IP {ip}:")
        print(result)
        grep_count = result.count('\n')
        print(f"Amount of lines pattern appeared : {grep_count}\n")
        cumulative += grep_count
        
        
        if "Errno" not in result:
            respondents +=1
            if not test_assert(grep_count == 1, "Pattern found on Machine " + str(index), "Pattern not found on Machine " + str(index)):
                return False
            
        index += 1
    
    print(f"Amount of lines pattern appeared accross all machines : {cumulative}")
    return test_assert(cumulative == respondents, "Pattern correctly found on all Machines", "Pattern was not found on all Machines")


def query_membership_list(ip, udp_port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2)
        message = 'GET_MEMBERSHIP'
        sock.sendto(message.encode('utf-8'), (ip, udp_port))
        data, _ = sock.recvfrom(4096)
        membership_data = data.decode('utf-8')
        print(f"{ip}:{udp_port}member list：\n{membership_data}")
    except Exception as e:
        print(f"when finding{ip}：{str(e)}")
    finally:
        sock.close()

#Main Function
if __name__ == "__main__":
    
    #Flag descriptions
    parser = argparse.ArgumentParser(description="Query multiple machines.")
    parser.add_argument('--grep', type=str, required=False, help='Query to execute on the machines')
    parser.add_argument('--test', type=int, required=False, help='Test number to run if not using --grep')
    parser.add_argument('--populate', type=bool, required=False, help='Generate Log Files For Unit Testing')

    # Parse the arguments
    args = parser.parse_args()
    
    #All the machines on the network
    # machines = [
    #     {'ip': '172.22.94.78', 'port': 9999},
    #     {'ip': '172.22.156.79', 'port': 9999},
    #     {'ip': '172.22.158.79', 'port': 9999},
    #     {'ip': '172.22.94.79', 'port': 9999},
    #     {'ip': '172.22.156.80', 'port': 9999},
    #     {'ip': '172.22.158.80', 'port': 9999},
    #     {'ip': '172.22.94.80', 'port': 9999},
    #     {'ip': '172.22.156.81', 'port': 9999},
    #     {'ip': '172.22.158.81', 'port': 9999},
    #     {'ip': '172.22.94.81', 'port': 9999},
    # ] 

    # hostlist = ['172.22.157.76', '172.22.159.77']

    hostlist = ['172.22.157.76', '172.22.159.77', '172.22.95.76', 
            '172.22.157.77', '172.22.159.78', '172.22.95.77', 
            '172.22.157.78', '172.22.159.79', '172.22.95.78', 
            '172.22.157.79']

    machines = [{'ip': ip, 'port': 8848} for ip in hostlist]

    
    #Checks the flags
    if (args.grep and args.test) or (args.grep and args.populate):
        parser.error("You can only use --populate and --test together.")
    
    #General Grep Request    
    if args.grep:
        # Execute the query on all machines
        results = grep_all_machines(machines, args.grep)
        
        # Print the aggregated results
        print_results(results)
        