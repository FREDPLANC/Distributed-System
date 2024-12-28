import sys
import json

def process_tuples(X):
    """
    处理输入数据：
    - 按逗号分隔 value，检查第七列是否是指定类型 X。
    - 如果第七列是 X，将第九列的值作为 key，输出 {"key": category, "value": 1}。
    - 特殊处理空值和单个空格。
    
    :param X: 指定的标志杆类型（第七列需要匹配的值）
    """
    try:
        for line in sys.stdin:
            record = json.loads(line.strip())  # 解析 JSON 数据
            value = record["value"]

            # 按逗号分隔 value
            columns = value.split(",")

            # 检查列数是否足够以及第七列是否匹配
            if len(columns) >= 9 and columns[6].strip() == X:
                # 获取第九列的值
                raw_category = columns[8]  # 原始值

                if raw_category == " ":
                    category = "Single Space"  # 单个空格
                elif raw_category.strip() == "":
                    category = "Empty String"  # 空值
                else:
                    category = raw_category.strip()  # 去掉两端空格的正常值

                # 输出结果
                result = {"key": category, "value": 1}
                sys.stdout.write(json.dumps(result) + "\n")
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: python category_counter_operator.py <X>\n")
        sys.exit(1)

    # 从命令行参数中获取 X
    X = sys.argv[1]
    process_tuples(X)