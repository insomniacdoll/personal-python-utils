#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

import sys

sys.path.append("./");

import logging
import log
from log import CommonLogger

STDOUT_FILE = "/dev/stdout"

class GlobalLogger(object):
    #默认向stderr输出
    def_logger = CommonLogger(STDOUT_FILE)
    logger = def_logger
    __is_open = bool(False);

    @staticmethod
    def open(log_file=STDOUT_FILE, mod_name="Mod", level=logging.DEBUG):
        GlobalLogger.logger = CommonLogger(log_file, mod_name, level)
        GlobalLogger.__is_open = bool(True)

    @staticmethod
    def close():
        del GlobalLogger.logger
        GlobalLogger.logger = GlobalLogger.def_logger
        GlobalLogger.__is_open = False





#/* vim: set expandtab ts=4 sw=4 sts=4 tw=100 */
