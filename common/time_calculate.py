#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com


import sys
from time import localtime, strftime

sys.path.append("./");

import logging
from datetime import datetime, timedelta

from gl_global_logger import GlobalLogger


class TimeCalculate(object):
    """
    时间戳计算器
    基于python内建的时间类型datetime，和时间差类型timedelta
    """
    __QUARTER_PERIOD = 0;
    __HOUR_PERIOD = 1;
    __DAY_PERIOD = 2;
    __WEEK_PERIOD = 3;
    __MONTH_PERIOD = 4;
    __TIMEFMT = "%Y%m%d%H%M%S"
    __TS_LEN = 14
    __QUARTER_LEN = 12
    __HOUR_LEN = 10
    __DAY_LEN = 8
    __MONTH_LEN = 6

    def __init__(self):
        """
        初始化时间戳对象
        """
        self.__init_period_delta()
        self.__init_ts_len()

    #时间戳示例20120704170129
    def __init_period_delta(self):
        """
        初始化不同时间周期（如刻钟, 小时，天，周，月）的单位时间(delta)
        例如：天周期的单位时间对应的是1天, 在python中的表示即为timedelta(minutes = 1)
        """
        self.__period_delta_dict = dict()
        self.__period_delta_dict[TimeCalculate.__QUARTER_PERIOD] = timedelta(minutes = 1) * 15;
        self.__period_delta_dict[TimeCalculate.__HOUR_PERIOD] = timedelta(hours = 1);
        self.__period_delta_dict[TimeCalculate.__DAY_PERIOD] = timedelta(days = 1);
        self.__period_delta_dict[TimeCalculate.__WEEK_PERIOD] = timedelta(weeks = 1);
        #timedelta不提供对month和year的支持
        #self.__period_delta_dict[TimeCalculate.__MONTH_PERIOD] = ;

    def __init_ts_len(self):
        """
        初始化不同时间周期的时间戳有效长度
        @ret void
        """
        self.__period_ts_len =  dict()
        self.__period_ts_len[TimeCalculate.__QUARTER_PERIOD] = TimeCalculate.__QUARTER_LEN
        self.__period_ts_len[TimeCalculate.__HOUR_PERIOD] = TimeCalculate.__HOUR_LEN
        self.__period_ts_len[TimeCalculate.__DAY_PERIOD] = TimeCalculate.__DAY_LEN
        self.__period_ts_len[TimeCalculate.__MONTH_PERIOD] = TimeCalculate.__MONTH_LEN

    def format_ts(self, input_ts, period):
        """
        根据统计周期period，格式化input_ts
        @ret 格式化后的字符串
        """
        #{{{
        try:
            #检查输入时间戳格式是否正确
            self.is_ts_valid(input_ts)

            ts_effect_len = self.__period_ts_len[period]
            effect_ts_head = input_ts[0:ts_effect_len]
            ts_tail_zero_len = TimeCalculate.__TS_LEN - ts_effect_len - 2

            if period == TimeCalculate.__MONTH_PERIOD:
                mid_str = "01"
            else:
                mid_str = "00"

            tail_zero_str = ""
            i = 0
            while i < ts_tail_zero_len:
                tail_zero_str += "0"
                i += 1

            fmt_str = effect_ts_head + mid_str + tail_zero_str

            self.is_ts_valid(fmt_str)

            if fmt_str > input_ts:
                raise Exception("Bad fmt_str:" + fmt_str)

            return fmt_str
        except Exception ,e:
            GlobalLogger.logger.warning("Exception msg:%s", e)
            raise
        #}}}

    def str_to_time(self, ts_str):
        """
        将字符串形式的时间戳, 转换为python内置的时间类型datetime
        """
        if len(ts_str) != TimeCalculate.__TS_LEN:
            raise Exception("Invalid ts_str[" + ts_str + "], len[" + str(len(ts_str) ) + "]")

        return datetime.strptime(ts_str, TimeCalculate.__TIMEFMT)

    def is_ts_valid(self, ts_str):
        self.str_to_time(ts_str)

    def is_period_valid(self, period):
        if period not in [TimeCalculate.__QUARTER_PERIOD, \
                TimeCalculate.__HOUR_PERIOD, TimeCalculate.__DAY_PERIOD, \
                TimeCalculate.__MONTH_PERIOD]:
            raise Exception("invalid period:", period)

    def inc_delta(self, ts_str, period, count=1):
        """
        对给定时间戳进行加、减操作，返回计算后的时间戳
        @ts_str: 输入时间戳
        @period: 时间戳的周期. 小时, 天, 月...
        @count: 需要变更多少个时间单位. 
                period = 天, count = 5, 表示在输入时间戳基础上加5天, \
                count = -5, 表示减5天
        """
        input_dt = self.str_to_time(ts_str) 
        #由于timedelta不支持month，所以需特殊处理
        if period != TimeCalculate.__MONTH_PERIOD:
            delta = self.__period_delta_dict[period] * count
            output_dt = input_dt + delta
        else:
            total_month = input_dt.month + count
            output_year = input_dt.year + total_month / 12
            output_month = total_month % 12

            if (output_month == 0):
                output_month = 12
                output_year -= 1
            output_dt = datetime(output_year, output_month, input_dt.day, \
                    input_dt.hour, input_dt.minute, input_dt.second)

        return output_dt.strftime(TimeCalculate.__TIMEFMT)

    def get_now_timestamp(self):
        """
        获取当前系统时间戳
        """
        timestamp = strftime(TimeCalculate.__TIMEFMT, localtime() )
        return timestamp

#全局时间戳计算器, 正常情况下使用该对象即可，无需自行声明时间戳对象
g_time_calculate = TimeCalculate()

#/* vim: set expandtab ts=4 sw=4 sts=4 tw=100 */
