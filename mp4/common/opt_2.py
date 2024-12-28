import sys
from collections import Counter

def process_stream(post_type):
    """
    """
    category_counter = Counter()

    try:
        for line in sys.stdin:
            key, value = line.strip().split("\t")  # 假设输入是以 tab 分隔的 key-value 对
            fields = value.split(",")  # 假设 value 是 CSV 格式

            sign_post_type = fields[2] if len(fields) > 2 else ""  # Sign Post Type
            category = fields[3] if len(fields) > 3 else ""        # Category

            if sign_post_type == post_type:
                category_counter[category] += 1
                sys.stdout.write(f": {dict(category_counter)}\n")

    except Exception as e:
        sys.stderr.write(f"error: {e}\n")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(1)

    post_type = sys.argv[1]
    process_stream(post_type)