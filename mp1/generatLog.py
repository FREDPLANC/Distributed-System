import random
import time
from faker import Faker

fake = Faker()

def generate_log_entry():
    ip_address = fake.ipv4()
    date_time = time.strftime('%d/%b/%Y:%H:%M:%S -0500', time.gmtime(random.randint(0, int(time.time()))))
    method = random.choice(["GET", "POST", "DELETE", "PUT", "PATCH"])
    endpoint = random.choice(["/list", "/posts/posts/explore", "/wp-admin", "/wp-content", "/blog", "/home"])
    protocol = "HTTP/1.0"
    status_code = random.choice([200, 301, 404, 500])
    bytes_sent = random.randint(4000, 6000)
    referrer = fake.url()
    user_agent = fake.user_agent()
    return f'{ip_address} - - [{date_time}] "{method} {endpoint} {protocol}" {status_code} {bytes_sent} "{referrer}" "{user_agent}"'

def write_logs_to_file(logs, filename):
    
    print("writting to", filename)
    
    with open(filename, 'w') as file:
        for log in logs:
            file.write(log + '\n')
    shuffle_file_lines(filename)

def generate_logs_with_user_string_mixed(num_entries, user_substring, logs):
    # logs = []
    for _ in range(num_entries):
        ip_address = fake.ipv4()
        date_time = time.strftime('%d/%b/%Y:%H:%M:%S -0500', time.gmtime(random.randint(0, int(time.time()))))
        method = random.choice(["GET", "POST", "DELETE", "PUT", "PATCH"])
        endpoint = random.choice(["/list", "/posts/posts/explore", "/wp-admin", "/wp-content", "/blog", "/home"])
        protocol = "HTTP/1.0"
        status_code = random.choice([200, 301, 404, 500])
        bytes_sent = random.randint(4000, 6000)
        referrer = fake.url()
        user_agent = fake.user_agent()
        if random.choice([True, False]):
            log = f'{ip_address} - - [{date_time}] "{method} {endpoint} {protocol}" {status_code} {bytes_sent} "{referrer}" "{user_agent}"'
        else:
            log = f'{ip_address} - - [{date_time}] "{method} {endpoint} {protocol}" {status_code} {bytes_sent} "{user_substring}" "{user_agent}"'

        logs.append(log)
    return logs

def generate_logs_with_user_string(num_entries, user_substring, logs):
    # logs = []
    for _ in range(num_entries):
        ip_address = fake.ipv4()
        date_time = time.strftime('%d/%b/%Y:%H:%M:%S -0500', time.gmtime(random.randint(0, int(time.time()))))
        method = random.choice(["GET", "POST", "DELETE", "PUT", "PATCH"])
        endpoint = random.choice(["/list", "/posts/posts/explore", "/wp-admin", "/wp-content", "/blog", "/home"])
        protocol = "HTTP/1.0"
        status_code = random.choice([200, 301, 404, 500])
        bytes_sent = random.randint(4000, 6000)
        referrer = fake.url()
        user_agent = fake.user_agent()
        log = f'{ip_address} - - [{date_time}] "{method} {endpoint} {protocol}" {status_code} {bytes_sent} "{user_substring}" "{user_agent}"'
        logs.append(log)
    
    return logs


def generate_logs(num_entries):
    logs = [generate_log_entry() for _ in range(num_entries)]
    return logs

def generate_rare_pattarn(num, user_strings, logs):
    newlog = generate_logs(num-10)
    for l in newlog:
        logs.append(l)
    generate_logs_with_user_string_mixed(10, user_strings, logs)
    return logs

def generate_frequent_pattern(num, user_strings, logs):
    newlog = generate_logs(int(num/8))
    for l in newlog:
        logs.append(l)
    generate_logs_with_user_string(3 * int(num/8), user_strings, logs)
    generate_logs_with_user_string_mixed(int(num/2), user_strings, logs)
    return logs
    
def append_somewhat_frequent_patter(num, filename,user_strings):
    appendlogs = []
    generate_logs_with_user_string(num, user_strings, appendlogs)
    append_lines_to_file(filename, appendlogs)
    shuffle_file_lines(filename)

def append_lines_to_file(filename, lines):
    try:
        with open(filename, 'a') as file:
            for line in lines:
                file.write(line + '\n')
        print(f"succes in {filename} ")
    except Exception as e:
        print(f"fail: {e}")

def shuffle_file_lines(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    random.shuffle(lines)
    with open(file_path, 'w') as file:
        file.writelines(lines)
