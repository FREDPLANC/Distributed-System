import random

def generate_txt(file_name, counts):
    """
    生成一个包含指定行数的文本文件，每行是 ['dog', 'cat', 'mouse'] 中的一个。
    
    :param file_name: 输出文件名
    :param counts: 一个字典，指定每种类别的数量，例如 {'dog': 150, 'cat': 100, 'mouse': 250}
    """
    # Flatten the counts dictionary into a list
    items = [key for key, count in counts.items() for _ in range(count)]
    random.shuffle(items)  # Shuffle the items randomly

    # Write to file
    with open(file_name, "w") as f:
        for item in items:
            f.write(f"{item}\n")
    print(f"File '{file_name}' with {sum(counts.values())} lines has been created.")

# Define counts for each category
counts = {'dog': 150, 'cat': 100, 'mouse': 250}

# Generate the file
generate_txt("countTest.txt", counts)