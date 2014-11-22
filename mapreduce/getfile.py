#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

from utils import *
import os
import hashlib
import sys
import shutil
import md5tool

def wget(remote_file, local_file, limite_rate='10M', retry_times=3):
    """@brief 将remote_file下载为本地的local_file
    @param remote_file 远程文件路径
    @param local_file 本地文件名
    @param limite_rate 最大下载速度
    """
    cmd = 'wget -qc --limit-rate=%s --tries=%d %s -O %s >&/dev/null' \
            %(limite_rate, retry_times, remote_file, local_file)
    run_cmd(cmd)

class Md5Error(Exception):
    pass

def wget_file_with_md5(remote_file, md5_file, temp_dir, target_dir, retry_times=3):
    """
    @brief 下载远程文件并且进行md5校验，如果remote_md5_file为'', 那么默认为
    远程文件名.md5, remote_md5_file为文件名不含路径
    如果md5没有变化则不会再次下载，如果md5不匹配则抛出Md5Error异常
    @param remote_file 远程文件路径，全路径ftp://user:passwd@192.168.11.11/a.txt
    @param remote_md5_file md5文件名 例如为a.txt.md5
    @param temp_dir 临时目录
    @param target_file 最终目录
    @param 0 表示文件没有变化 1文件有变化
    """
    remote_dir = os.path.dirname(remote_file)
    file_name = os.path.basename(remote_file)
    remote_md5_file = ''
    if not md5_file:
        md5_file = file_name + '.md5'

    remote_md5_file = os.path.join(remote_dir, md5_file)

    temp_file = os.path.join(temp_dir, file_name + '.tmp')
    temp_md5_file = os.path.join(temp_dir, md5_file + '.tmp')

    target_file = os.path.join(target_dir, file_name)
    target_md5_file = os.path.join(target_dir, md5_file)

    for i in range(retry_times):
        # 下载md5文件
        wget(remote_md5_file, temp_md5_file)
        # 如果md5值没有变化则直接返回
        if os.path.exists(target_md5_file):
            status = os.system('diff %s %s' %(temp_md5_file, target_md5_file))
            if status == 0:
                logging.info('File %s not changed' %remote_file)
                return 0
        # md5有变化则重新下载
        wget(remote_file, temp_file)
        # 检查md5
        local_md5 = md5tool.md5sum(temp_file)
        if local_md5 != open(temp_md5_file).read().split()[0]:
            if i == retry_times-1:
                raise Md5Error('%s md5sum doesn\'t match' %remote_file)
            else:
                # 当md5不同时，等待60s在进行下载
                time.sleep(60)
                continue
        shutil.move(temp_file, target_file)
        shutil.move(temp_md5_file, target_md5_file)
        return 1

