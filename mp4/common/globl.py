HOSTLIST = [
            '172.22.157.76', 
            '172.22.159.77', 
            '172.22.95.76', 
            '172.22.157.77', 
            '172.22.159.78', 
            '172.22.95.77', 
            '172.22.157.78', 
            '172.22.159.79', 
            '172.22.95.78', 
            '172.22.157.79'
        ]
STAGE_NAMES = {1: "Spout", 2: "Bolt1", 3: "Bolt2"}
'''
        task_request = {
            "type": "Transform",
            "task_id": "Task_Spout_1",
            "operator": "count",
            "data": {"oreder", "create"}
            "topology": {'Task_Sout_1': ['vm1#12345', 'vm2#12345'], 'Task_Bolt_1': ['vm3#12345', 'vm4#12345'], 'Task_Bolt_2': ['vm5#12345', 'vm6#12345']}
            "partition": (start, end)
            "source": "/home/twei11/ga0/mp4/hydfs_local_get",
            "dest": "/home/twei11/ga0/mp4/hydfs_local_get",
        }
'''

TASKIDMAP = {'Task_Spout_1', 'Task_Spout_2', 'Task_Spout_3', 'Task_Bolt1_1', 'Task_Bolt1_2', 'Task_Bolt1_3', 'Task_Bolt2_1', 'Task_Bolt2_2', 'Task_Bolt2_3'}

MEMBERSHIP_PORT = 9999
HYDFS_PORT = 10000

NIMBUS_PORT = 12345
WOKER_PORT = 12345

TOPOLOGY_PORT = 13245

HYDFS_LOCAL_DIR = "/home/twei11/ga0/mp4/hydfs_local_get/"

STREAMING_PORT = 12346

ACK_PORT = 12367

OPT_DIR = "/home/twei11/ga0/mp4/common/build/"
INPUT_DIR = "/home/twei11/ga0/mp4/data/"
OUPUT_DIR = "/home/twei11/ga0/mp4/"

STREAMING_PORT_DIC = {'Task_Spout_1': 14770, 'Task_Spout_2': 14771, 'Task_Spout_3': 14772, 'Task_Bolt1_1': 14773, 'Task_Bolt1_2': 14774, 'Task_Bolt1_3': 14775, 'Task_Bolt2_1': 14776, 'Task_Bolt2_2': 14777, 'Task_Bolt2_3': 14778}

NIMBUS_STREAM_IP = '172.22.157.76'
NIMBUS_STREAM_PORT = 15000

NIMBUS_RECIEVE_PORT = 19187
