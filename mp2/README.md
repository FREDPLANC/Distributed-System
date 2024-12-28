# MP2 Project: Distributed Group Membership
In our MP2 implementation, we mainly drew inspiration from the SWIM protocol's dissemination mechanism, specifically the PING-ACK node information exchange process. In the design, each node periodically pings a random set of K nodes (from the full membership list) and expects an ACK in return to check if the node has left or failed. If there are updates to a node's status, instead of immediately sending extra update messages, the node waits until the next ping cycle to attach the update information to the PING/ACK message. 

In our implementation, we attached the updated membership list to the outgoing messages. This approach prevents the network from being overloaded, ensures that the load is balanced across all nodes, and provides an efficient mechanism for failure detection and information dissemination.

As for suspection mode, we use two tic-tok to measure the time it takes to receive a feedback from a specific node. If a node is labelled as a suspected node, it will be added into a pending list and wait for timer to clear them if no further update arrives.

## How to Run the Project

1. **Run Introducer on introducer VM**:

     ```
    python server.py [server ip] [server port] None PingAck
     ```

2. **Run other nodes on other VMs**:
   - You can issue a `grep` query from **any VM** by typing the following in the terminal:
     ```
        python server.py [server ip] [server port] [introducer_ip:introducer_port] PingAck
     ```

## Commands during running:

- list_mem: list the membership list

-list_self: list self’s id


- leave: voluntarily leave the group (different from a failure)

- enable/disable suspicion (enable_sus / disable_sus)

- suspicion on/off status (status_sus)


## Other Files
- we can also use the quray machine implement in MP1, by running client.py -grep "grep [something]"

