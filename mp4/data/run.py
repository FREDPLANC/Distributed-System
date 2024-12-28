import csv
import json
import subprocess


def read_csv_to_tuples(input_csv, output_file):
    """
    从 CSV 文件读取数据并生成 JSON 格式的 tuples，每行一个。
    将结果写入 output_file。
    
    :param input_csv: 输入 CSV 文件路径
    :param output_file: 输出 JSON 文件路径
    """
    try:
        with open(input_csv, "r") as infile, open(output_file, "w") as outfile:
            reader = csv.reader(infile)
            for row in reader:
                line = ",".join(row)  # 将每行内容连接成字符串
                tuple_json = {"key": -1, "value": line}  # 构造 JSON 格式的 tuple
                outfile.write(json.dumps(tuple_json) + "\n")  # 写入文件
        print(f"Formatted input written to {output_file}")
    except Exception as e:
        print(f"Error processing CSV file: {e}")


def run_operator(input_file, operator_script, output_file, operator_args=None):
    """
    调用 operator 脚本处理数据。
    
    :param input_file: 输入 JSON 文件路径
    :param operator_script: operator 脚本路径
    :param output_file: 输出文件路径
    :param operator_args: 传递给 operator 的参数列表
    """
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
                print(f"Output from {operator_script} written to {output_file}")
    except Exception as e:
        print(f"Error running operator {operator_script}: {e}")


def main():
    """
    主流程：
    - 从 CSV 文件读取数据，生成 JSON 格式的输入。
    - 调用第一个 operator 进行处理。
    - 调用第二个 operator 进行聚合。
    - 将最终结果写入 local.record 文件。
    """
    # 输入与输出文件路径
    input_csv = "Traffic_Signs_10000.csv"
    formatted_input = "formatted_input.json"
    intermediate_output = "intermediate_output.json"
    final_output = "local.record"
    
    # Operator 脚本路径
    operator_1 = "../common/op_app2_1.py"  # 第一个 operator
    operator_2 = "../common/op_app2_2.py"  # 第二个 operator

    # 参数
    X = "Streetlight"  # 第一个 operator 的参数

    # 步骤 1: 从 CSV 文件读取数据并格式化
    print("Step 1: Reading CSV and formatting input...")
    read_csv_to_tuples(input_csv, formatted_input)

    # 步骤 2: 调用第一个 operator
    print("Step 2: Running the first operator...")
    run_operator(formatted_input, operator_1, intermediate_output, [X])

    # 步骤 3: 调用第二个 operator
    print("Step 3: Running the second operator...")
    run_operator(intermediate_output, operator_2, final_output)

    print(f"Process completed. Final output written to {final_output}")


if __name__ == "__main__":
    main()