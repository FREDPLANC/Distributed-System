import sys
import json

def filter_tuples(pattern):
    """
    """
    try:
        for line in sys.stdin:
            record = json.loads(line.strip())  
            key = record["key"]
            value = record["value"]

            if pattern in value:
                sys.stdout.write(json.dumps({"key": key, "value": value}) + "\n")
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: python filter_operator.py <pattern>\n")
        sys.exit(1)

    pattern = sys.argv[1]
    filter_tuples(pattern)