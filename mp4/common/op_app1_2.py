import sys
import json

def process_tuples():
    """
    """
    try:
        for line in sys.stdin:
            record = json.loads(line.strip())  
            key = record["key"]
            value = record["value"]


            columns = value.split(",")
            new_value = ",".join([columns[2].strip(), columns[3].strip()])


            result = {"key": key, "value": new_value}
            sys.stdout.write(json.dumps(result) + "\n")
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    process_tuples()