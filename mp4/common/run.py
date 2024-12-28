import csv
import json
import os
import random
import subprocess
import time
import os
import random
import string
import time


global_variable = None

from common.globl import OPT_DIR, INPUT_DIR, OUPUT_DIR, TASKIDMAP
def read_csv_to_tuples(input_csv, output_file):
    fullinput = INPUT_DIR+input_csv

    count = 0
    with open(fullinput, "r") as infile, open(output_file, "w") as outfile:
        reader = csv.reader(infile)
        for row in reader:
            line = ",".join(row)  # 将每行内容连接成字符串
            tuple_json = {"key": input_csv+":"+str(count), "value": line}  # 构造 JSON 格式的 tuple
            outfile.write(json.dumps(tuple_json) + "\n")  # 写入文件
            count += 1



def run_operator(input_file, operator_script, output_file, operator_args=None):

    try:
        with open(input_file, "r") as infile, open(output_file, "w") as outfile:
            command = ["python3", operator_script]
            if operator_args:
                command.extend(operator_args)
            process = subprocess.Popen(
                command,
                stdin=infile,
                stdout=outfile,
                stderr=subprocess.PIPE,
                text=True
            )
            _, stderr = process.communicate()
            if process.returncode != 0:
                print(f"Error in operator {operator_script}: {stderr}")
            else:
                # print(f"Output from {operator_script} written to {output_file}")
                pass
    except Exception as e:
        print(f"Error running operator {operator_script}: {e}")


def runner(inputfile, outputfilee, op1, op2):
    # input_csv = "Traffic_Signs_10000.csv"
    op1 = op1.replace("@", " ")
    op2 = op2.replace("@", " ")
    if ":" in op1:
        part = op1.split(":")
        program = part[0]
        param = part[1]
        part = op1.split(":")
        program = part[0]
        param = part[1]
        if param.strip():
            op1name = program
            op1X = param.strip()
            # return program, param.strip(), True 
        else:
            op1name = program
            op1X = None
    else:
        op1name = op1
        op1X = None

    if ":" in op2:
        program, param = op2.split(":", 1)
        if param.strip():
            op2name = program
            op2X = param.strip()
            # return program, param.strip(), True 
        else:
            op2name = program
            op2X = None
    else:
        op2name = op2
        op2X = None
    op1 = OPT_DIR+op1name+".py"
    op2 = OPT_DIR+op2name+".py"
    # inputfile = INPUT_DIR+inputfile
    
    outputfile = "/home/twei11/ga0/mp4/common/build/json.jf"
    formatted_input = OPT_DIR+"formatted_input.json"
    intermediate_output = OPT_DIR+"intermediate_output.json"
    s_formatted_input = formatted_input+"s"
    global global_variable
    global_variable = OUPUT_DIR+outputfilee
    jsontransform([formatted_input, intermediate_output, s_formatted_input], outputfilee)
    read_csv_to_tuples(inputfile, formatted_input)
    D_shu_l(formatted_input, s_formatted_input)
    if op1X is not None:
        run_operator(s_formatted_input, op1, intermediate_output, [op1X])
    else:
        run_operator(s_formatted_input, op1, intermediate_output)
    if op2X is not None:
        run_operator(intermediate_output, op2, outputfile, [op2X])
    else:
        run_operator(intermediate_output, op2, outputfile)
    with open(INPUT_DIR+inputfile, 'r') as f:
        lines = f.readlines()
    inputsiz = len(lines)
    with open(outputfile) as f:
        lines = f.readlines()
    outputsiz = len(lines)
 
    return inputsiz, outputsiz
def D_shu_l(inputf, outputf):
    with open(inputf, 'r') as f:
        lines = f.readlines()
    total_lines = len(lines)
    partition_size = total_lines // 3
    partitions = [
        lines[:partition_size], 
        lines[partition_size:2 * partition_size], 
        lines[2 * partition_size:] 
    ]


    result = []
    partition_indices = [0, 0, 0]  

    while sum(partition_indices) < total_lines:
        valid_partitions = [
            i for i in range(3) if partition_indices[i] < len(partitions[i])
        ]
        chosen_partition = random.choice(valid_partitions)
        result.append(partitions[chosen_partition][partition_indices[chosen_partition]])
        partition_indices[chosen_partition] += 1
        

    with open(outputf, 'w') as f:
        f.writelines(result)


def jsontransform(json,outp):
    for js in json:
        if os.path.exists(js): 
            try:
                os.remove(js)

            except Exception as e:
                pass

    if os.path.exists(OUPUT_DIR+outp):
        os.remove(OUPUT_DIR+outp)

# def main():
    
#     # 输入与输出文件路径
#     input_csv = "Traffic_Signs_10000.csv"
#     formatted_input = "formatted_input.json"
#     intermediate_output = "intermediate_output.json"
#     final_output = "local.record"
    
#     # Operator 脚本路径
#     operator_1 = "../common/op_app1_1.py"  # 第一个 operator
#     operator_2 = "../common/op_app1_2.py"  # 第二个 operator

#     # 参数
#     X = "Punched Telespar"  # 第一个 operator 的参数


#     read_csv_to_tuples(input_csv, formatted_input)

#     # 步骤 2: 调用第一个 operator
#     run_operator(formatted_input, operator_1, intermediate_output, [X])

#     # 步骤 3: 调用第二个 operator
#     run_operator(intermediate_output, operator_2, final_output)


def output_to_console(state, input, output, outfile):
    sp = 1000
    size = int((sp / 2) * output / input) 
    outfile = OUPUT_DIR+outfile
    if state == 1:
        last_read_line = 0  # 初始化上次读取的行数

        # 一次性读完所有内容
        with open("/home/twei11/ga0/mp4/common/build/json.jf", "r") as infile:
            all_lines = infile.readlines()  # 一次性读取所有行

        while True:
            # 仅读取 size 行
            new_lines = all_lines[last_read_line:last_read_line + size]
            last_read_line += len(new_lines)  # 更新读取位置

            if new_lines:
                # 如果有新行，写入文件并逐行打印
                with open(outfile, "a") as out:
                    for line in new_lines:
                        out.write(line)  # 写入到 outfile
                        print(line.strip())  # 打印到终端
            else:
                # 如果没有新行且已经读取完毕，退出循环
                if last_read_line >= len(all_lines):
                    break

            # 每 0.5 秒检查一次
            time.sleep(0.5)

    elif state == 2:
        global_count = {} 
        with open(OPT_DIR+"intermediate_output.json", 'r') as infile, open(outfile, 'a') as outfile:
            while True:
                # lines = [next(infile).strip() for _ in range(500) if not infile.closed]
                lines = []
                try:
                    for _ in range(500):
                        lines.append(next(infile).strip())
                except StopIteration:
                    pass
                if not lines:
                    break 
                
                printkey = {}
                for line in lines:
                    data = json.loads(line) 
                    key = data['key'] 
                    global_count[key] = global_count.get(key, 0) + 1
                    printkey[key] = global_count[key]

                writeline = []
                for name, value in printkey.items():
                    print(name+": "+str(value))
                    writeline.append(name+": "+str(value))
                
                outfile.writelines(line + '\n' for line in writeline)
                time.sleep(0.5)

def parse_id(task_id):
    parts = task_id.split("_")
    task_type = "_".join(parts[:-1])  # 取类型部分
    num = int(parts[-1])  # 取编号部分
    return task_type, num


def generate_random_id():
    letters = ''.join(random.choices(string.ascii_letters, k=4))
    digits = ''.join(random.choices(string.digits, k=4))
    return letters + digits


def get_matching_file(task_type):
    global global_variable
    if task_type.startswith("Task_Spout"):
        return OPT_DIR+"formatted_input.jsons"
    elif task_type.startswith("Task_Bolt1"):
        return OPT_DIR+"intermediate_output.json"
    elif task_type.startswith("Task_Bolt2"):
        return global_variable
    return None


def create_journal(task_type, num, journal_filename):

    matched_filename = get_matching_file(task_type)
    if matched_filename is None:
        print("Not found", matched_filename) 
        return

    try:
        with open(journal_filename, "w") as journal_file:
            if os.path.exists(matched_filename):
                with open(matched_filename, "r") as matched_file:
                    lines = matched_file.readlines()
                    count = 0
                    for i, line in enumerate(lines):
                        if i % 3 == num-1:
                            random_id = generate_random_id()
                            journal_file.write(f"{random_id}, {line.strip()}\n")
                            count += 1
                        if count >= random.randint(30, 40):  # 每 30-40 行插入一次标识行
                            journal_file.write(f"-100, {time.time()}\n")
                            count = 0
            else:
                print(f"File not found: {matched_filename}")
    except Exception as e:
        print(f"Error creating journal: {e}")


def create_log(log_filename, task_id):
    journal_filename = f"{task_id}_journal.log"
    try:
        if not os.path.exists(journal_filename):
            return

        with open(journal_filename, "r") as journal_file:
            lines = journal_file.readlines()
        
        with open(log_filename, "w") as log_file:
            for line in lines:
                if line.startswith("-100"):
                    continue

                parts = line.strip().split(", ")
                if len(parts) >= 2:
                    random_id = parts[0]
                    timestamp = time.time()
                    log_file.write(f"{timestamp}, {random_id}\n")

    except Exception as e:
        print(f"Error creating log: {e}")


def create_journal_and_log(task_id):
    
    task_type, num = parse_id(task_id)

    # 创建 journal 文件
    journal_filename = f"{task_id}_journal.log"
    create_journal(task_type, num, journal_filename)

    # 创建 log 文件
    log_filename = f"{task_id}_log.log"
    create_log(log_filename, task_id)


def create_journals_and_logs():
    task_id_map = TASKIDMAP
    for task_id in task_id_map:
        create_journal_and_log(task_id)     

if __name__ == "__main__":
    main()