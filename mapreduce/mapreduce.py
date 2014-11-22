#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

import os
import sys
import utils
import logging

def _parse_template(text, **kargs):
    """
    @brief 将text模板所表示的字符串绘制出来
    @param text 模板字符串
    @param kargs 模板中的参数
    @return 替换后的字符串
    """
    from string import Template
    s = Template(text)
    return s.substitute(**kargs)

def make_mapreduce_cmd(hadoop_bin, job_conf, **kargs):
    """
    @brief 根据Job描述的hadoop任务生成hadoop命令
    @param job
    @param kargs 模板参数
    """
    mapreduce_parameters = job_conf['mapreduce']
    parameters = []
    parameters.append('%s streaming' %hadoop_bin)
    for key, value in mapreduce_parameters.items():
        if isinstance(value, list):
            for item in value:
                parameters.append('%s "%s"' %(key,  _parse_template(item, **kargs)))
        else:
            parameters.append('%s "%s"' %(key, _parse_template(value, **kargs)))
    cmd = ' \\\n'.join(parameters)
    return '%s 1>>%s 2>>%s' %(cmd, job_conf['local_log'], job_conf['local_log'])

def run_job(hadoop_bin, job_conf, date_str):
    """ 根据任务配置job，运行日期date_str执行mapreduce任务 """
    output_path = _parse_template(job_conf['mapreduce']['-output'][0], DATE=date_str)
    try:
        logging.info('Delete directory %s' %output_path)
        cmd = '%s fs -rmr %s' %(hadoop_bin, output_path)
        logging.info('Delete dir:%s' %cmd)
        utils.run_cmd(cmd)
    except Exception:
        logging.warning('Delete directory %s failed' %output_path)
    cmd_str =  make_mapreduce_cmd(hadoop_bin, job_conf, DATE=date_str)
    utils.run_cmd(cmd_str)

