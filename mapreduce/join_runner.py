#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

import sys
import os
from utils import *

"""������streaming����join��mapreduce���"""

def get_mapper_feeds(stdin):
    """ ˳���ȡ�����ļ� """
    for line in stdin:
        yield line.strip('\n')

def run_mapper(mapper):
    """ map���̣�ʹ��mapper���������map���� """
    for feed in get_mapper_feeds(sys.stdin):
        keyvalue=None
        try:
            for keyvalue in mapper.do_map(feed):
                print keyvalue
        except:
            Error("invalid input [%s]" %keyvalue);

class FileDescriptor(object):
    def __init__(self, pattern, tag):
        """ @param pattern��os.environ['map_input_file'] �������ж����ĸ������ļ�
            @param tag: �ڲ���joinʹ�������ж����ĸ������ļ�
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
        @brief ͨ�õĶ��ļ�Map���
        @param mappers : mapper����ÿ��mapper��Ӧ��һ�������ļ�
        """
        self.mappers = []
    def add_mapper(self, pattern, mapper):
        """
        @brief ����һ�� �ļ����͵�mapper
        @param pattern �����ļ�����
        @param mapper pattern��Ӧ��map����
        """
        inner_mapper = self.InnerMapper(pattern, mapper)
        self.mappers.append(inner_mapper)
    def _check_input_file_name(self):
        """
        @brief ���������ļ�a�Ƿ������Ӧ��mapper
        """
        found = False
        for mapper in self.mappers:
            if mapper.pattern in os.environ['map_input_file']:
                found = True
        return found

    def do_map(self, line):
        """ ����<os.environ['map_input_file'], line> ������Ӧ��mapper���д��� """
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
        @brief ������join��ͨ�ÿ��
        @param tags ��ʾ��¼���͵�tag�б�
        """
        self.tags = []

    def add_tag(self, tag):
        self.tags.append(tag)

    def set_reducer(self, reducer):
        self.reducer = reducer

    def combine_bykeys(self, stdin):
        """
        @breif ��reduce��������ϳ� <key, [v1, v2, v3]>����ʽ��������ı�дreduce
        ������key�����������л��
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
        ��Ϊ���һ����Ҫ��ָ����unitid����reduceͰ��ȡģ��ɢ�е���Ӧ��part�У�
        �� ÿ����¼ǰ����һ�� ID + �ո�, ���Դ˴���������
        @breif ��reduce��������ϳ� <key, [v1, v2, v3]>����ʽ��������ı�дreduce
        ������key�����������л��
        """
        current_key = None
        current_values = {}
        for tag in self.tags:
            current_values[tag] = []

        for orgline in stdin:
            try:
                #ȥ��\n ����ȥ����һ�������Ͱkey 
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
    """ reduce���̣�ʹ��tags��JoinReduce����tags���л�ۣ�reducerΪreduce���� 
      �����¼�ĸ�ʽ�ǣ�key\ttag\tOther fields
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
    """ reduce���̣�ʹ��tags��JoinReduce����tags���л�ۣ�reducerΪreduce���� 
      �����¼�ĸ�ʽ�ǣ�key\ttag\tOther fields
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
