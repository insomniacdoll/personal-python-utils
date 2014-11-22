#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

import sys
import os
from utils import *

"""������streaming�� ��mapreduce���
�����Ҫjoin��μ�join_runner.py
"""

def run_mapper(mapper):
    for line in sys.stdin:
        line = line.strip('\n')
        keyvalue = None
        try:
            for keyvalue in mapper(line):
                print keyvalue
        except Exception, e:
            Error(e)
            Error('Invalid input [%s]' %keyvalue)

def combine_bykeys(stdin):
    """
    @breif ��reduce��������ϳ� <key, [v1, v2, v3]>����ʽ��������ı�дreduce
    ������key�����������л��
    """
    current_key = None
    current_values = []

    for line in stdin:
        line = line.strip('\n')
        (key, value) = line.split('\t', 1)
        if current_key == key:
            current_values.append(value)
        else:
            if current_key:
                yield current_key, current_values
            current_key = key
            current_values = [value]

    if current_key != None and current_key == key:
        yield current_key, current_values

def run_reducer(reducer):
    for key, values in combine_bykeys(sys.stdin):
        try:
            for keyvalue in reducer(key, values):
                print keyvalue
        except Exception, e:
            Error(e)

