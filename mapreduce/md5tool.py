#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

from utils import *
import os
import hashlib
import sys
import shutil

def _sumfile(fobj):
    m = hashlib.md5()
    while True:
        d = fobj.read(8096)
        if not d:
            break
        m.update(d)
    return m.hexdigest()

def md5sum(fname):
    """ �����ļ���md5
    @return �ļ���MD5ֵ������ļ��������򷵻�None
    """
    ret = None
    if fname == '-':
        ret = _sumfile(sys.stdin)
    else:
        f = open(fname, 'rb')
        ret = _sumfile(f)
        f.close()
    return ret

def gen_md5(fname, md5fname):
    """
    @brief �ȼ���md5sum fname > md5fname
    """
    md5_str = md5sum(fname)
    f = open(md5fname, 'w')
    f.write('%s  %s\n' %(md5_str, os.path.basename(fname)))
    f.close()

def check_md5(fname, md5fname):
    raise Exception('Not implement method')
