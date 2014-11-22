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
    @brief ��textģ������ʾ���ַ������Ƴ���
    @param text ģ���ַ���
    @param kargs ģ���еĲ���
    @return �滻����ַ���
    """
    from string import Template
    s = Template(text)
    return s.substitute(**kargs)

def make_mapreduce_cmd(hadoop_bin, job_conf, **kargs):
    """
    @brief ����Job������hadoop��������hadoop����
    @param job
    @param kargs ģ�����
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
    """ ������������job����������date_strִ��mapreduce���� """
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

