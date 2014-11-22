#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com
 
 
import sys
import os
import logging

import MySQLdb


class MysqlHandler(object):
    '''mysql数据库操作封装类'''#{{{

    def __init__(self, host, port, user, passwd, database, logger):
        '''初始化'''#{{{
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.database = database
        self.logger = logger#}}}

    def conn_to_db(self):
        '''连接到数据库'''#{{{
        try:
            self.dbconn = MySQLdb.connect(host=self.host, port=self.port, 
                    user=self.user, passwd=self.passwd, db=self.database)
            self.logger.debug('success to connect to mysql database, '
                    '[host:%s], [port=%s], [user=%s], [passwd=%s], [db:%s], ', 
                    self.host, self.port, self.user, self.passwd, 
                    self.database)

        except BaseException, e:
            self.logger.error('failed to connect to mysql database, '
                    '[host:%s], [port=%s], [user=%s], [passwd=%s], [db:%s], '
                    '[exception_desc:%s]', self.host, self.port, self.user, 
                    self.passwd, self.database, e)
            raise#}}}

    def disconn_from_db(self):
        '''从数据库断开连接'''#{{{
        try:
            self.dbconn.close()
            self.logger.debug('success to disconnect from mysql database, '
                    '[host:%s], [port=%s]', self.host, self.port)

        except BaseException, e:
            self.logger.warning('failed to disconnect from mysql database, '
                    '[host=%s], [port=%s], [exception_desc:%s]',
                    self.host, self.port, e)#}}}

    def exec_select_sql(self, sql):
        '''执行查询sql，返回结果集'''#{{{
        try:
            #connect to db
            self.conn_to_db()

            #set autocommit
            self.dbconn.autocommit(True)

            #get cursor
            self.cursor = self.dbconn.cursor()

            #execute sql
            self.cursor.execute(sql)

            #get result set
            result = self.cursor.fetchall()

            self.logger.debug('success to execute select sql, '
                    '%s rows returned, [host:%s], [port:%s], [sql:%s]', 
                    len(result), self.host, self.port, sql)

            #close cursor
            self.cursor.close()

            #disconnect to db
            self.disconn_from_db()

            return result

        except BaseException, e:
            self.logger.error('failed to execute select sql, '
                    '[host:%s], [port:%s], [sql:%s], [exception_desc:%s]', 
                    self.host, self.port, sql, e)
            self.disconn_from_db()
            raise#}}}

    def exec_update_sql(self, sql):
        '''执行更新sql，返回影响的行数'''#{{{
        try:
            #connect to db
            self.conn_to_db()

            #get cursor
            self.cursor = self.dbconn.cursor()

            #execute sql
            num_of_affect_rows = self.cursor.execute(sql)

            #commit
            self.dbconn.commit()

            self.logger.debug('success to execute update sql, '
                    '%s rows affected, [host:%s], [port:%s], [sql:%s]', 
                    num_of_affect_rows, self.host, self.port, sql)

            #close cursor
            self.cursor.close()

            #disconnect to db
            self.disconn_from_db()

            return num_of_affect_rows

        except BaseException, e:
            self.logger.error('failed to execute update sql, '
                    '[host:%s], [port:%s], [sql:%s], [exception_desc:%s]', 
                    self.host, self.port, sql, e)
            self.disconn_from_db()
            raise#}}}

    def exec_transaction(self, sql_list):
        '''执行一个transaction'''#{{{
        try:
            #connect to db
            self.conn_to_db()

            #get cursor
            self.cursor = self.dbconn.cursor()

            #start transaction
            self.dbconn.begin()

            #execute sqls
            for sql in sql_list:
                self.cursor.execute(sql)
                self.logger.debug('success to execute one sql in '
                        'the transaction, [sql:%s], [sql_list:%s]', 
                        sql, sql_list)

            #commit
            self.dbconn.commit()

            self.logger.debug('success to execute the transaction, [host:%s], '
                    '[port:%s], [sql_list:%s]', self.host, self.port, sql_list)

            #close cursor
            self.cursor.close()

            #disconnect to db
            self.disconn_from_db()

            return True

        except BaseException, e:
            self.logger.error('failed to execute the transaction, '
                    '[host:%s], [port:%s], [sql_list:%s], [exception_desc:%s]', 
                    self.host, self.port, sql_list, e)
            self.disconn_from_db()
            raise#}}}
#}}}


#/* vim: set ts=4 sw=4 sts=4 tw=100 expandtab: */
