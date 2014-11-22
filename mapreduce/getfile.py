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
    """@brief ��remote_file����Ϊ���ص�local_file
    @param remote_file Զ���ļ�·��
    @param local_file �����ļ���
    @param limite_rate ��������ٶ�
    """
    cmd = 'wget -qc --limit-rate=%s --tries=%d %s -O %s >&/dev/null' \
            %(limite_rate, retry_times, remote_file, local_file)
    run_cmd(cmd)

class Md5Error(Exception):
    pass

def wget_file_with_md5(remote_file, md5_file, temp_dir, target_dir, retry_times=3):
    """
    @brief ����Զ���ļ����ҽ���md5У�飬���remote_md5_fileΪ'', ��ôĬ��Ϊ
    Զ���ļ���.md5, remote_md5_fileΪ�ļ�������·��
    ���md5û�б仯�򲻻��ٴ����أ����md5��ƥ�����׳�Md5Error�쳣
    @param remote_file Զ���ļ�·����ȫ·��ftp://user:passwd@192.168.11.11/a.txt
    @param remote_md5_file md5�ļ��� ����Ϊa.txt.md5
    @param temp_dir ��ʱĿ¼
    @param target_file ����Ŀ¼
    @param 0 ��ʾ�ļ�û�б仯 1�ļ��б仯
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
        # ����md5�ļ�
        wget(remote_md5_file, temp_md5_file)
        # ���md5ֵû�б仯��ֱ�ӷ���
        if os.path.exists(target_md5_file):
            status = os.system('diff %s %s' %(temp_md5_file, target_md5_file))
            if status == 0:
                logging.info('File %s not changed' %remote_file)
                return 0
        # md5�б仯����������
        wget(remote_file, temp_file)
        # ���md5
        local_md5 = md5tool.md5sum(temp_file)
        if local_md5 != open(temp_md5_file).read().split()[0]:
            if i == retry_times-1:
                raise Md5Error('%s md5sum doesn\'t match' %remote_file)
            else:
                # ��md5��ͬʱ���ȴ�60s�ڽ�������
                time.sleep(60)
                continue
        shutil.move(temp_file, target_file)
        shutil.move(temp_md5_file, target_md5_file)
        return 1

