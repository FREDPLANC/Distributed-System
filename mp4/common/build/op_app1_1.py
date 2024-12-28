import sys
import json

def filter_tuples(pattern):
    """
    过滤输入数据，判断每个 tuple 的 value 是否包含指定的 pattern。
    
    :param pattern: 需要匹配的字符串
    """
    try:
        for line in sys.stdin:
            record = json.loads(line.strip())  # 解析 JSON 数据
            key = record["key"]
            value = record["value"]

            # 检查是否包含指定的 pattern
            if pattern in value:
                sys.stdout.write(json.dumps({"key": key, "value": value}) + "\n")
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: python filter_operator.py <pattern>\n")
        sys.exit(1)

    # 从命令行参数中获取 pattern
    pattern = sys.argv[1]
    filter_tuples(pattern)