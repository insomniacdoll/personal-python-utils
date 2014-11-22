#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Author : insomniacdoll@gmail.com

import sys
import os
import traceback
import logging
import socket
import time
import commands

def ErrorExit(msg):
    """ Print message to stderr and exit."""
    sys.stderr.write("%s\n" %(msg))
    traceback.print_exc()
    logging.fatal(traceback.format_exc())
    logging.fatal(msg)
    sys.exit(1)

def Error(msg):
    """ Print message to stderr."""
    sys.stderr.write("%s\n" %(msg))
    traceback.print_exc()
    logging.fatal(traceback.format_exc())
    logging.warning(msg)

    #    1        2     3        4            5        6        7           8
    #-rw-r--r--   3    user     user   73136542 2014-09-21 10:35 /home/hiyoru/data/
class ManifestMaker:
    '''创建@manifest文件, 本地生产后put到集群，使用本地临时路径 ./tmp_manifest'''
    def __init__(self, hadoop_path, manifest_path):
        self.hadoop_path=hadoop_path.strip();
        self.manifest_path = manifest_path
        self.tmp_manifest="%s/@manifest" %(self.manifest_path)

    def get_local_manifest_file(self):
        try:
            if not os.path.exists(self.manifest_path):
                os.mkdir(self.manifest_path);
            local_manifest=file(self.tmp_manifest, "wb+");
        except Exception,e:
            Error(e)
            Error("create local file[%s] failed1" %(self.tmp_manifest));
            return None
        return local_manifest

    def put_manifest_to_hdfs(self, hdfs_dir):
        target="%s/@manifest" %(hdfs_dir)
        ret = os.system("%s/bin/hadoop fs -test -e %s" %(self.hadoop_path,target))
        if 0 == int(ret) :
            rm_ret = os.system("%s/bin/hadoop fs -rm %s" %(self.hadoop_path,target))
            if 0 != int(rm_ret):
                Error("rm file [%s] failed!" %(target));
                return False
        ret = os.system("%s/bin/hadoop fs -put %s %s/" %(self.hadoop_path,
                         self.tmp_manifest, target))
        if 0 != int(ret):
            Error("put @manifest [%s] failed!" %(target));
            return False
        else:
            logging.info("put @manifest [%s] succeed!" %(target));
            return True

    def create_local_manifest(self, local_manifest, hdfs_dir):
        lines = os.popen("%s/bin/hadoop fs -ls %s/" %(self.hadoop_path,
        hdfs_dir)).readlines()
        if lines:
            try:
                for fline in lines:
                    fline=fline.strip()
                    length=len("Found ")
                    if 0 == int(cmp(fline[:length], "Found "[:length])):
                        continue
                    length=len("@manifest")
                    if 0 == int(cmp(fline[-length:], "@manifest"[-length:])):
                        continue
                    #print "%s" %(fline.strip())
                    #logging.info("%s" %(fline.strip()))
                    #获取ls的文件大小和时间
                    file_info = fline.split()
                    fbytes = file_info[4]
                    ftime = str("%s %s" %(file_info[5], file_info[6]))
                    fpath = file_info[7]
                    fpath = fpath.replace(hdfs_dir, "");
                    while fpath and fpath[0] == os.sep:
                        fpath = fpath[1:]
                    manifest_str = "%s\t%s\t%s\n" %(ftime, fbytes, fpath)
                    local_manifest.write(manifest_str);
            except Exception, e:
                Error(e);
                Error(fline);
                return False
        return True

    def create_manifest(self, hdfs_dir):
        local_manifest=self.get_local_manifest_file()
        if not local_manifest:
            return False;

        if not self.create_local_manifest(local_manifest, hdfs_dir):
            local_manifest.close()
            return False
        else:
            local_manifest.close()

        if not self.put_manifest_to_hdfs(hdfs_dir):
            Error("put @manifest [%s] failed!" %(hdfs_dir));
            return False
        else:
            logging.info("create @manifest [%s] succeed!" %(hdfs_dir));
            return True

class ManifestChecker:
    '''用@manifest校验一个文件夹中的所有文件'''
    def __init__(self, hadoop_path):
        self.hadoop_path=hadoop_path.strip();
    def check(self, manifest_path):
        retval=True;
        try:
            lines = os.popen("%s/bin/hadoop fs -cat %s/@manifest" %(self.hadoop_path,
            manifest_path)).readlines();
            if lines:
                for fline in lines:
                    m_line = ManifestLine(fline.strip())
                    if not m_line.check_file(self.hadoop_path, manifest_path):
                        Error("check manifest failed!");
                        retval=False;
                        break
            else:
                retval=False;
        except Exception,e:
            Error("path=%s, check manifest faild!" %(self.hadoop_path));
            Error(e);
            retval=False
        return retval;

    def simple_check(self, manifest_path):
        retval=True;
        try:
            ret = os.system("%s/bin/hadoop fs -test -e %s/@manifest" %(self.hadoop_path,
            manifest_path))
            if 0 == int(ret):
                retval = True;
                logging.info("simple_check @manifest [%s] succeed!" %(manifest_path));
            else:
                retval = False;
                Error("simple_check @manifest [%s] failed!" %(manifest_path));
        except Exception, e:
            Error(e);
            Error("simple_check @manifest [%s] failed!" %(manifest_path));
            retval = False;
        finally:
            return retval;

class ManifestLine:
    def __init__(self, line):
        info=line.strip().split("\t");
        self.time = info[0];
        self.bytes = int(info[1]);
        self.file_path = info[2];

    #    1        2     3        4            5        6        7           8
    #-rw-r--r--   3    user     user   73136542 2014-09-21 10:35 /home/hiyoru/data/s
    def check_file(self, hadoop_path, manifest_path):
        '''校验@manifest的一行文件在集群上的大小是否和描述匹配'''
        retval=True;
        try:
            lines = os.popen("%s/bin/hadoop fs -ls %s/%s" %(hadoop_path,
            manifest_path, self.file_path)).readlines()[1];
            #print "%s" %(lines)
            #获取ls的文件大小和时间
            file_info = lines.strip().split();
            fbytes = int(file_info[4])
            ftime = str("%s %s" %(file_info[5], file_info[6]))
            if self.time != ftime:
                Error("file[%s] check failed!" %(lines));
                Error("manifest:%s, really:%s check modifytime failed!" %(self.time, ftime));
                retval=False;
            elif self.bytes != fbytes:
                retval=False;
                Error("file[%s] check failed!" %(lines));
                Error("manifest:%s, really:%s check bytes failed!" %(self.bytes, fbytes));
            return retval;
        except Exception, e:
            Error(e);
            logging.warning(traceback.format_exc())
            return False;

    def __str__(self):
        print "ManifestLine:[%s] [%s] [%s]" %(self.time, self.bytes,
        self.file_path);


def set_log_name(fname):
    logging.basicConfig(
            filename = fname,
            format='%(asctime)s %(levelname)s [%(filename)s][%(lineno)d][%(funcName)s] %(message)s',
            datefmt='%y/%m/%d %H:%M:%S',
            level=logging.DEBUG)

def run_cmd(command_str):
    (status, output) = commands.getstatusoutput(command_str)
    if status != 0:
        #logging.warning('%s failed' %command_str)
        raise RuntimeError('%s failed [%d] msg[%s]' %(command_str, status, output))
    return (status, output)

def hadoop_path_size(hadoop_path, path):
    """获得集群上目录的大小
    """
    hadoop_bin = '%s/bin/hadoop' %hadoop_path
    command = '%s fs -du %s' %(hadoop_bin, path)
    (status, output) = commands.getstatusoutput(command)
    size = 0
    if status == 0:
        for line in output.split('\n'):
            if not line.startswith('Found'):
                (fsize, fname) = line.split()
                size += int(fsize)
    else:
        logging.warning('%s failed' %command)
    return size

def combine_log(hadoop_path, hadoop_temp_log_path, local_log_file, hadoop_log_path):
    ONE_GIGABYTES = 1024 * 1024 * 1024
    hadoop_bin = '%s/bin/hadoop' %hadoop_path
    fsize = hadoop_path_size(hadoop_path, hadoop_temp_log_path)
    if fsize > ONE_GIGABYTES:
        try:
            logging.error('%s too many error logs' %hadoop_temp_log_path)
            run_cmd('%s fs -mv %s/* %s' %(hadoop_bin, hadoop_temp_log_path, hadoop_log_path))
            run_cmd('%s fs -rm %s/*' %(hadoop_bin, hadoop_temp_log_path))
        except RuntimeError:
            logging.warning('merge log for %s failed' %hadoop_temp_log_path)
            return False
    else:
        if fsize != 0:
            hadoop_file = os.path.join(hadoop_log_path, os.path.basename(local_log_file))
            try:
                s = os.system('%s fs -rm %s' %(hadoop_bin, hadoop_file))
                if s != 0:
                    logging.warning('remove %s failed' %hadoop_file)
                run_cmd('%s fs -cat %s/* > %s' %(hadoop_bin, hadoop_temp_log_path, local_log_file))
                run_cmd('%s fs -put %s %s' %(hadoop_bin, local_log_file, hadoop_log_path))
                run_cmd('%s fs -rm %s/*' %(hadoop_bin, hadoop_temp_log_path))
            except RuntimeError, e:
                logging.warning(e)
                logging.warning('merge log for %s failed' %hadoop_temp_log_path)
                return False
    return True

def save_log(hadoop_path, confpath, logname, dest_path):
    """ 这个函数在hadoop集群上执行，每次map/reduce完，将错误日志上传到dest_path目录
    """
    hadoop_bin = '%s/bin/hadoop' %hadoop_path
    fname = '%s_%s_%s' %(socket.gethostname(), time.strftime('%Y%m%d%H%M%S'), logname)
    local_log = os.path.join('.', logname)
    remote_log = os.path.join(dest_path, fname)
    for i in range(0, 3):
        try:
            run_cmd('%s --config %s fs -put %s %s' %(hadoop_bin, confpath, local_log, remote_log))
            return
        except RuntimeError:
            pass
        time.sleep(1)

def get_conf_path():
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), '../conf')

class HadoopFileSystem(object):
    def __init__(self, hadoop_home):
        self.hadoop_bin = '%s/bin/hadoop' %hadoop_home

    def mkdir(self, path):
        cmd = '%s fs -test -d %s' %(self.hadoop_bin, path)
        s = os.system(cmd)
        if s != 0:
            cmd = '%s fs -mkdir %s' %(self.hadoop_bin, path)
            run_cmd(cmd)

    def upload(self, local_file, target_dir):
        """
        @brief 将本地文件上传到hadoop
        @param local_file 本地文件
        @param target_dir 可以为路径
        """
        filename = os.path.basename(local_file)
        hadoop_temp_file = target_dir + '/' + filename + '.tmp'
        hadoop_target_file = target_dir + '/' + filename;
        cmd = '%s fs -rm %s' %(self.hadoop_bin, hadoop_temp_file)
        os.system(cmd)
        cmd = '%s fs -rm %s' %(self.hadoop_bin, hadoop_target_file)
        os.system(cmd)
        cmd = '%s fs -put %s %s' %(self.hadoop_bin, local_file, hadoop_temp_file)
        run_cmd(cmd)
        cmd = '%s fs -mv %s %s' %(self.hadoop_bin, hadoop_temp_file, hadoop_target_file)
        run_cmd(cmd)

    def exists(self, file_path):
        """
        @brief 判断文件是否存在
        """
        cmd = '%s fs -test -e %s' %(self.hadoop_bin, file_path)
        s = os.system(cmd)
        if s == 0:
            return True
        else:
            return False

    def rmr(self, path):
        """
        @brief 删除hdfs上的path目录
        """
        cmd = '%s fs -rmr %s' %(self.hadoop_bin, path)
        run_cmd(cmd)

    def cat(self, path, out_file_name):
        """
        @brief cat path的内容到out_file中 path可以为文件名，也可以为 '/app/*'
        """
        cmd = '%s fs -cat %s > %s' %(self.hadoop_bin, path, out_file_name)
        run_cmd(cmd)

