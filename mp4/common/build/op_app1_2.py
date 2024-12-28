import sys
import json

def process_tuples():
    """
    处理输入数据：
    - 将每个 tuple 的 value 按逗号分隔。
    - 提取第三列和第四列的数据。
    - 以逗号连接第三列和第四列，生成新的 value。
    - 输出新的 tuple。
    """
    try:
        for line in sys.stdin:
            record = json.loads(line.strip())  # 解析 JSON 数据
            key = record["key"]
            value = record["value"]

            # 按逗号分隔 value 并提取第三列和第四列
            columns = value.split(",")
            new_value = ",".join([columns[2].strip(), columns[3].strip()])

            # 构造新 tuple 并输出
            result = {"key": key, "value": new_value}
            sys.stdout.write(json.dumps(result) + "\n")
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    process_tuples()