#!/bin/bash

# 删除 hydfs_storage 文件夹及其内容
if [ -d "hydfs_storage" ]; then
    rm -rf hydfs_storage
    echo "hydfs_storage 文件夹及其内容已删除."
else
    echo "hydfs_storage 文件夹不存在."
fi

# 删除 hydfs_cache 文件夹及其内容
if [ -d "hydfs_cache" ]; then
    rm -rf hydfs_cache
    echo "hydfs_cache 文件夹及其内容已删除."
else
    echo "hydfs_cache 文件夹不存在."
fi

# 清空 local_log.log 文件内容
if [ -f "local_log.log" ]; then
    > local_log.log
    echo "local_log.log 文件内容已清空."
else
    echo "local_log.log 文件不存在."
fi