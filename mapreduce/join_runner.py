#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

import sys
import os
from utils import *

"""适用于streaming进行join的mapreduce框架"""

def get_mapper_feeds(stdin):
    """ 顺序读取输入文件 """
    for line in stdin:
        yield line.strip('\n')

def run_mapper(mapper):
    """ map过程：使用mapper对输入进行map运算 """
    for feed in get_mapper_feeds(sys.stdin):
        keyvalue=None
        try:
            for keyvalue in mapper.do_map(feed):
                print keyvalue
        except:
            Error("invalid input [%s]" %keyvalue);

class FileDescriptor(object):
    def __init__(self, pattern, tag):
        """ @param pattern：os.environ['map_input_file'] 中用来判断是哪个输入文件
            @param tag: 内部供join使用用来判断是哪个输入文件
        """
        self.pattern = pattern
        self.tag = tag

class JoinMapper(object):
    class InnerMapper:
        def __init__(self, pattern, mapper):
            self.pattern = pattern
            self.func = mapper
    def __init__(self):
        """
        @brief 通用的多文件Map框架
        @param mappers : mapper链表，每个mapper对应于一个输入文件
        """
        self.mappers = []
    def add_mapper(self, pattern, mapper):
        """
        @brief 增加一个 文件类型的mapper
        @param pattern 输入文件类型
        @param mapper pattern对应的map函数
        """
        inner_mapper = self.InnerMapper(pattern, mapper)
        self.mappers.append(inner_mapper)
    def _check_input_file_name(self):
        """
        @brief 对于输入文件a是否存在相应的mapper
        """
        found = False
        for mapper in self.mappers:
            if mapper.pattern in os.environ['map_input_file']:
                found = True
        return found

    def do_map(self, line):
        """ 对于<os.environ['map_input_file'], line> 调用相应的mapper进行处理 """
        if '@manifest' in os.environ['map_input_file']:
            sys.exit(0)

        if not self._check_input_file_name():
            ErrorExit('Unkown input file %s' %os.environ['map_input_file'])

        for mapper in self.mappers:
            if mapper.pattern in os.environ['map_input_file']:
                try:
                    for keyvalue in mapper.func(line):
                        yield keyvalue
                except (ValueError, RuntimeError, Exception) as e:
                    Error(os.environ['map_input_file'])
                    Error(e)

class JoinReducer(object):
    def __init__(self):
        """
        @brief 适用于join的通用框架
        @param tags 表示记录类型的tag列表
        """
        self.tags = []

    def add_tag(self, tag):
        self.tags.append(tag)

    def set_reducer(self, reducer):
        self.reducer = reducer

    def combine_bykeys(self, stdin):
        """
        @breif 将reduce的输入组合成 <key, [v1, v2, v3]>的形式，更方便的编写reduce
        即按照key对输入流进行汇聚
        """
        current_key = None
        current_values = {}
        for tag in self.tags:
            current_values[tag] = []

        for line in stdin:
            line = line.strip('\n')
            (key, record_tag, data) = line.split('\t', 2)
            if current_key == key:
                found_tag = False
                for tag in self.tags:
                    if tag == record_tag:
                        found_tag = True
                        current_values[record_tag].append(data)
                        break
                if not found_tag:
                    Error('Unkown tag [%s], data [%s]' %(record_tag, line))
                    return
            else:
                if current_key:
                    yield current_key, current_values
                current_key = key
                current_values = {}
                for tag in self.tags:
                    current_values[tag] = []
                current_values[record_tag].append(data)

        if current_key != None and current_key == key:
            yield current_key, current_values

    def combine_partioned_bykeys(self, stdin):
        """
        因为最后一轮需要将指定的unitid按照reduce桶数取模，散列到对应的part中，
        且 每条记录前面有一个 ID + 空格, 所以此处单独处理
        @breif 将reduce的输入组合成 <key, [v1, v2, v3]>的形式，更方便的编写reduce
        即按照key对输入流进行汇聚
        """
        current_key = None
        current_values = {}
        for tag in self.tags:
            current_values[tag] = []

        for orgline in stdin:
            try:
                #去掉\n 并且去掉第一个排序分桶key 
                line = orgline.strip('\n').split(" ", 1)[1]
                (key, record_tag, data) = line.split('\t', 2)
                if current_key == key:
                    found_tag = False
                    for tag in self.tags:
                        if tag == record_tag:
                            found_tag = True
                            current_values[record_tag].append(data)
                            break
                    if not found_tag:
                        Error('Unkown tag [%s], data [%s]' %(record_tag, line))
                        return
                else:
                    if current_key:
                        yield current_key, current_values
                    current_key = key
                    current_values = {}
                    for tag in self.tags:
                        current_values[tag] = []
                    current_values[record_tag].append(data)
            except Exception, e:
                Error("invalid line:[%s]" %orgline);

        if current_key != None and current_key == key:
            yield current_key, current_values

def run_reducer(tags, reducer):
    """ reduce过程，使用tags让JoinReduce按照tags进行汇聚，reducer为reduce函数 
      输入记录的格式是：key\ttag\tOther fields
    """
    join_reducer = JoinReducer()
    for tag in tags:
        join_reducer.add_tag(tag)
    for key, values in join_reducer.combine_bykeys(sys.stdin):
        try:
            for keyvalue in reducer(key, values):
                print keyvalue
        except (ValueError, RuntimeError, Exception) as e:
            Error(e)

def run_partition_reducer(tags, reducer):
    """ reduce过程，使用tags让JoinReduce按照tags进行汇聚，reducer为reduce函数 
      输入记录的格式是：key\ttag\tOther fields
    """
    join_reducer = JoinReducer()
    for tag in tags:
        join_reducer.add_tag(tag)
    for key, values in join_reducer.combine_partioned_bykeys(sys.stdin):
        try:
            for keyvalue in reducer(key, values):
                print keyvalue
        except (ValueError, RuntimeError, Exception) as e:
            Error(e)
