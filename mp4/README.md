RainStorm
We build a simple version of RainStorm system with the code from MP2 and MP3. 

From MP2, we rebuild a failure detector to enable fault tolerance in the process of stream. 

From MP3, we incorporated each node with a HydfsNode to handle file system operation, including managing distributed shared files like logs and journals.

We use Nimbus in master node, to coordinate all the work among all the workers. 

We use Supervisor in worker nodes, to run the streaming job. Each follower has a config (or topology) with current job. They are no need to get whole views of the picture. Just do the partial job and send result to the child node

Usage:

Run python3 mp4/runNimbus.py on Leader introducer
Run python3 mp4/worker_main.py on Worker Node

Then all the nodes are prepared to do any stream jobs consists of two ops.

The ops are custom operations build from the python code. In our design, you are free to use any operators from common/dist to complete the job.


Now all the machines are launched in the rainstorm system

RainStorm <op1 _exe> <op2 _exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>