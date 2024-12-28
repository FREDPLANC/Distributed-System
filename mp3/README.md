Distributed File SysteM
This repository presents a simple distributed file system which can be used to store, read, and delete files across multiple machines. Data in the HYDFS is re-replicated when machines fail.

Usage:


Run python3 mp3/hydfs_node_merge.py from every machine. 

list_mem will print the membership list at any point.

store will print the files stored in the SDFS.

ls hydfs_filename prints the VM addresses where hydfs_filename is stored.

create local_filename filename adds a local file into the HYDFS.

get sdfs_filename local_filename get a file 

mutiappend starts a append operation on all VMs listed (VM1, VM2, VM3, VM4)


enable suspicion will switch from gossip mode to gossip+suspicion mode for failure detection.

disable suspicion will switch back to gossip mode for failure detection.

Any failures (and suspected failures) will be detected and printed on the terminal. Logs for debugging are also populated on every machine.