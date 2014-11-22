#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

import sys
import os
import logging
import logging.handlers


#@note logging level
#   CRITICAL   50 
#   ERROR      40
#   WARNING    30 
#   INFO       20
#   DEBUG      10
class CommonLogger(object):
    __NOTICE_LVL = 25
    __FATAL_LVL = 55
    __TRACE_LVL = 15
    __srcfile = sys._getframe(1).f_code.co_filename
    __counter = 0
    __STDERR_PATH = "/dev/stderr"
    __STDOUT_PATH = "/dev/stdout"


    def __init__(self, log_file=__STDOUT_PATH, mod_name="Mod", level=logging.DEBUG):
        # 日志等级和日志格式。包括FATAL, WARNING, NOTICE, TRACE, DEBUG，以及ERROR
        if log_file == CommonLogger.__STDOUT_PATH:
            wf_log_file = CommonLogger.__STDERR_PATH
        else:
            wf_log_file = log_file + ".wf";

        logging.addLevelName(CommonLogger.__NOTICE_LVL, "NOTICE")
        logging.addLevelName(CommonLogger.__FATAL_LVL, "FATAL")
        logging.addLevelName(CommonLogger.__TRACE_LVL, "TRACE")

        #logging.basciConfig(filename=log_file, format)

        #fmt_str = '%(levelname)s: %(asctime)s: %(name)s * %(process)d:%(thread)d [%(filename)s:%(lineno)s, '\
        #'%(funcName)s] %(message)s'
        fmt_str = '%(levelname)s: %(asctime)s: %(name)s * %(process)d:%(thread)d * %(levelname)s: %(message)s'
        fmt = logging.Formatter(fmt_str)

        self.file_hd = logging.handlers.WatchedFileHandler(log_file)
        self.file_hd.setFormatter(fmt)
        self.file_hd.setLevel(level)

        self.wf_file_hd = logging.handlers.WatchedFileHandler(wf_log_file)
        self.wf_file_hd.setFormatter(fmt)
        self.wf_file_hd.setLevel(logging.WARNING)

        self.normal_logger = logging.getLogger(mod_name + str(CommonLogger.__counter) )
        CommonLogger.__counter += 1

        self.wf_logger = logging.getLogger(mod_name + str(CommonLogger.__counter) )
        CommonLogger.__counter += 1

        self.normal_logger.addHandler(self.file_hd)
        self.normal_logger.setLevel(level)

        self.wf_logger.addHandler(self.wf_file_hd)
        self.wf_logger.setLevel(logging.WARNING)

        #打印日志起始的分割线
        div_str = "="
        div_str = div_str * 20
        start_str = div_str + " log start " + div_str
        self.notice(start_str)
        self.warning(start_str)

    def __del__(self):
        try:
            div_str = "="
            div_str = div_str * 20
            end_str = div_str + " log end " + div_str
            self.notice(end_str)
            self.warning(end_str)
            del self.normal_logger
            del self.wf_logger
            del self.file_hd
            del self.wf_file_hd
        except Exception, e:
            pass

    @staticmethod
    def code_msg(msg, fn, lno, func):
        return msg + "[" + fn + ":" + str(lno) + ", " + func + "]"

    def debug(self, msg, *args, **kwargs):
        fn, lno, func = self.__findCaller()
        msg_with_code_info = CommonLogger.code_msg(msg, fn, lno,func)
        self.normal_logger.log(logging.DEBUG, msg_with_code_info, *args, **kwargs)

    def trace(self, msg, *args, **kwargs):
        fn, lno, func = self.__findCaller()
        msg_with_code_info = CommonLogger.code_msg(msg, fn, lno,func)
        self.normal_logger.log(CommonLogger.__TRACE_LVL, msg_with_code_info, *args, **kwargs)

    def notice(self, msg, *args, **kwargs):
        fn, lno, func = self.__findCaller()
        msg_with_code_info = CommonLogger.code_msg(msg, fn, lno,func)
        self.normal_logger.log(CommonLogger.__NOTICE_LVL, msg_with_code_info, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        fn, lno, func = self.__findCaller()
        msg_with_code_info = CommonLogger.code_msg(msg, fn, lno,func)
        self.wf_logger.log(logging.WARNING, msg_with_code_info, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        fn, lno, func = self.__findCaller()
        msg_with_code_info = CommonLogger.code_msg(msg, fn, lno,func)
        self.wf_logger.log(logging.ERROR, msg_with_code_info, *args, **kwargs)

    def fatal(self, msg, *args, **kwargs):
        fn, lno, func = self.__findCaller()
        msg_with_code_info = CommonLogger.code_msg(msg, fn, lno,func)
        self.wf_logger.log(CommonLogger.__FATAL_LVL, msg_with_code_info, *args, **kwargs)

    def __currentframe(self):
        """
        返回调用栈的栈帧对象
        """
        try:
            raise Exception
        except:
            return sys.exc_info()[2].tb_frame.f_back

    def __findCaller(self):
        """
        寻找栈帧对象，用于定位源代码的文件名，行号以及函数名
        """
        f = self.__currentframe()
        #On some versions of IronPython, currentframe() returns None if
        #IronPython isn't run with -X:Frames.
        if f is not None:
            f = f.f_back
        rv = "(unknown file)", 0, "(unknown function)"
        while hasattr(f, "f_code"):
            co = f.f_code
            filename = os.path.normcase(co.co_filename)
            #print "__srcfile:%s" % CommonLogger.__srcfile
            #print "filename:%s" % filename
            if filename == CommonLogger.__srcfile:
                f = f.f_back
                continue
            rv = (co.co_filename, f.f_lineno, co.co_name)
            break
        return rv


#/* vim: set expandtab ts=4 sw=4 sts=4 tw=100 */
