# operator.py
import sys
import json
from collections import defaultdict

def process_stream():
    """
    从标准输入读取 JSON 数据流，按 key 聚合 value。
    输入数据为换行分隔的 JSON，每行格式为 {"key": ..., "value": ...}。
    输出数据为换行分隔的 JSON，每行格式为 {"key": ..., "value": ...}。
    """
    aggregated_results = defaultdict(int)

    try:
        # 从标准输入读取数据
        for line in sys.stdin:
            record = json.loads(line.strip())  # 解析 JSON 数据
            key = record[0]
            value = record[1]
            aggregated_results[key] += value  # 按 key 聚合 value

        # 输出聚合结果
        for key, value in aggregated_results.items():
            sys.stdout.write(json.dumps({"key": key, "value": value}) + "\n")
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    process_stream()