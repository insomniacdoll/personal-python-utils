#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

import sys
import os
from utils import *

"""适用于streaming的 简单mapreduce框架
如果需要join请参见join_runner.py
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
    @breif 将reduce的输入组合成 <key, [v1, v2, v3]>的形式，更方便的编写reduce
    即按照key对输入流进行汇聚
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

