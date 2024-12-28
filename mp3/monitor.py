import subprocess
import time

def monitor_bandwidth():
    cmd = "iftop -t -s 5" 
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    # print(result.stdout)
    with open("bandwidth_usage.log", "a") as f:
        f.write(result.stdout)

def monitor_cpu_memory():

    cmd = "top -b -n 1" 
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    with open("cpu_memory_usage.log", "a") as f:
        f.write(result.stdout)

if __name__ == "__main__":
    while True:
        monitor_bandwidth()        # monitor_cpu_memory()
        time.sleep(1)