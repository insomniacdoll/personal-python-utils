#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

import sys
import fcntl


class FileLock(object):
    # 公用文件锁模块, 用以保证单实例运行

    def __init__(self, lock_file):
        self.__is_lock = 0
        self.__lock_fp = open(lock_file, "w")
        fcntl.flock(self.__lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.__is_lock = 1

    def __del__(self):
        if self.__is_lock == 1:
            fcntl.flock(self.__lock_fp, fcntl.LOCK_EX)
            self.__lock_fp.close()
            self.__is_lock = 0




#/* vim: set expandtab ts=4 sw=4 sts=4 tw=100 */
