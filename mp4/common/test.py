import sys

def echo_stream():
    """
    """
    try:
        for line in sys.stdin:
            key, value = line.strip().split("\t")  #
            result = f"{key}\t{value}\n"
            sys.stdout.write(result)  #
    except Exception as e:
        sys.stderr.write(f"error: {e}\n")

if __name__ == "__main__":
    echo_stream()