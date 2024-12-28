# MP1 Project: Distributed Log Query System

This project implements a **client-server architecture** where every machine (VM) acts as both a client and a server. Each machine can send `grep` queries to search logs across all VMs in the system.

## How to Run the Project

1. **Run MP1 on Each VM**:
   - The system requires that **every VM** runs the `mp1.py` file.
   - Once the program is running on each VM, the following prompt will appear:
     ```
     Enter 'grep <pattern>' to search logs or 'exit' to quit:
     ```
   - This indicates the program is ready for queries.

2. **Send a Query**:
   - You can issue a `grep` query from **any VM** by typing the following in the terminal:
     ```
     grep <pattern>
     ```
   - The query will be sent to all VMs in the `hostlist`, and each VM will search its local logs. The matching logs will be stored in a directory named with the **current timestamp**.
   - Inside this directory, you will find log files named `vm1.txt` to `vm10.txt`, each containing the matching logs for that specific VM.

## Modifying VM IP Addresses

- If you need to run the project on VMs **outside the assigned range (VM01-VM10)**, you will need to modify the `hostlist` in the `mp1.py` file with the correct IP addresses of the 10 VMs you wish to use.

## Other Files

- **`unittest1.py`**: This file contains unit tests to verify the connections and ensure the system is functioning as expected.
  
- **`clearstuff.py`**: This script deletes folders with a specific naming pattern. We use it to clean up log directories created by the system.

- **`acctest.py`** and **`reportTest.py`**: These are acceptance test scripts. They validate whether the returned log contents meet the expected requirements and store the results in a query report located in the `test_result` directory.

## Important Notes

- The log files created by the system during queries are stored in the **current working directory**, under a folder named with the timestamp when the query was made.
  
- The program automatically handles failures when a VM is unresponsive, and logs the issue without affecting the rest of the queries.
