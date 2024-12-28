import os
import glob
import shutil

def delete_folders():
    # 匹配当前目录下所有以20240914开头的文件夹
    folders = glob.glob('20240915*')

    # 删除每一个匹配的文件夹
    for folder in folders:
        if os.path.isdir(folder):  # 确保它是一个文件夹
            try:
                shutil.rmtree(folder)  # 递归删除文件夹及其内容
                print(f"Deleted folder: {folder}")
            except Exception as e:
                print(f"Failed to delete folder {folder}: {e}")


# 执行删除操作
delete_folders()
