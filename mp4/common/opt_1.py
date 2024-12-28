import sys

def process_stream(pattern):
    """
    """
    try:
        for line in sys.stdin:
            key, value = line.strip().split("\t")  
            if pattern in value:
                fields = value.split(",")  
                object_id = fields[0] if len(fields) > 0 else ""  # OBJECTID
                sign_type = fields[1] if len(fields) > 1 else ""  # Sign_Type
                result = f"{key}\t{object_id},{sign_type}\n"
                sys.stdout.write(result)  # 
    except Exception as e:
        sys.stderr.write(f"error: {e}\n")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(1)

    pattern = sys.argv[1]
    process_stream(pattern)