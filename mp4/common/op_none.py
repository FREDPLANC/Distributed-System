# operator.py
import sys
import json

def process_stream():
    """
    从标准输入读取 JSON 数据流，处理数据，去掉 value 的左右空格并返回新的元组。
    输入数据为换行分隔的 JSON，每行格式为 {"key": ..., "value": ...}。
    输出数据为换行分隔的 JSON，每行格式为 {"key": ..., "value": (str, 1)}。
    """
    try:
        # 从标准输入逐行读取
        for line in sys.stdin:
            record = json.loads(line.strip())  # 解析 JSON 数据

            key = record[0]
            value = record[1].strip()  # 去掉左右空格
            result = {"key": value, "value": 1}  # 构造新的结果
            sys.stdout.write(json.dumps(result) + "\n")  # 输出新的 JSON 数据
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    process_stream()