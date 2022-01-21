# !/usr/bin/python
# -*- coding:utf-8 -*-
# author: gongxiaoma
# date： 2021-10-20
# version：1.0
# zkpython最晚更新时间2012年，kazoo去年还有更新。

import os
import sys
import json
import logging
import logging.config
import platform
from kazoo.client import KazooClient


standard_format = '[%(asctime)s][%(levelname)s][%(message)s]'
simple_format = '[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d]%(message)s'

windows_logfile_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__))) + '\logs'
linux_logfile_dir = "/tmp"
logfile_name = "kafka_topic_partition_sync_check-error.log"



class RunLog(object):
    """
    日志类，
    将日志记录到指定文件中
    """

    """定义实例变量"""
    def __init__(self, windows_logfile_dir, linux_logfile_dir, logfile_name):
        self.windows_logfile_dir = windows_logfile_dir
        self.linux_logfile_dir = linux_logfile_dir
        self.logfile_name = logfile_name


    """定义日志路径"""
    def logfile_path(self):
        sys = platform.system()
        if sys == "Windows":
            logfile_dir = self.windows_logfile_dir
        else:
            logfile_dir = self.linux_logfile_dir
        if not os.path.isdir(logfile_dir):
            os.makedirs(logfile_dir)
        logfile_path = os.path.join(logfile_dir, self.logfile_name)
        return logfile_path


    """定义logging日志字典"""
    def logging_dict(self):
        logfile_path = self.logfile_path()
        logging_config_dict = {
                           'version': 1,
                           'disable_existing_loggers': False,
                           'formatters': {
                               'standard': {
                                   'format': standard_format
                               },
                               'simple': {
                                   'format': simple_format
                               },
                           },
                           'filters': {},
                           'handlers': {
                               # 打印DEBUG级别日志到终端屏幕
                               'console': {
                                   'level': 'DEBUG',
                                   'class': 'logging.StreamHandler',
                                   'formatter': 'simple'
                               },
                               # 打印DEBUG级别日志到文件
                               'default': {
                                   'level': 'DEBUG',
                                   'class': 'logging.handlers.RotatingFileHandler',
                                   'formatter': 'standard',
                                   'filename': logfile_path,
                               'maxBytes': 1024 * 1024 * 5,
                               'backupCount': 5,
                               'encoding': 'utf-8',
                           },
                       },
                       'loggers': {
                                      # logging.getLogger(__name__)的logger配置，handlers可以根据自己情况设置
                                      '': {
                                          'handlers': ['default'],
                                          'level': 'INFO',
                                          'propagate': True,
                                      },
                                  },
        }
        return logging_config_dict


    """日志写入方法"""
    def logfile_write(self):
        logging_dict = self.logging_dict()
        logging.config.dictConfig(logging_dict)
        logger = logging.getLogger(__name__)
        return logger



class KafkaSyncCheck(object):
    '''
    类变量
    '''
    zk_topic_path = "/brokers/topics"


    '''
    构造函数：初始化生产环境和灾备环境zookeeper服务器地址
    '''
    def __init__(self, prd_zkaddress, dre_zkaddress):
        self.prd_zkaddress = prd_zkaddress
        self.dre_zkaddress = dre_zkaddress


    '''
    类方法：连接zookeeper服务器，直接返回执行结果
    '''
    @classmethod
    def connect_zk(cls, getmethod, zkaddress):
        try:
            zk = KazooClient(hosts=zkaddress)
            zk.start()
            if getmethod == "zk.get":
                result = zk.get(KafkaSyncCheck.zk_topic_path)
            else:
                result = zk.get_children(KafkaSyncCheck.zk_topic_path)
            return result
        except Exception as e:
            error_msg = {'status': '失败', 'message': '连接zookeeper服务器{}异常，请检查：{}'.format(zkaddress, e)}
            logger.error(json.dumps(error_msg, ensure_ascii=False))
            raise e
        finally:
            zk.stop()


    '''
    类方法：对比所有TOPIC的分区数量形成字典，方便后面对比
    '''
    @classmethod
    def get_partition_dict(cls, partition_list, zkaddress):
        try:
            partition_dict = {}
            zk = KazooClient(hosts=zkaddress)
            zk.start()
            for i in partition_list:
                result = zk.get_children(KafkaSyncCheck.zk_topic_path + "/" + i + "/partitions")
                partition_dict[i] = len(result)
            return partition_dict
        except Exception as e:
            error_msg = {'status': '失败', 'message': '连接zookeeper服务器{}异常，请检查：{}'.format(zkaddress, e)}
            logger.error(json.dumps(error_msg, ensure_ascii=False))
            raise e
        finally:
            zk.stop()



    '''
    实例方法：获取所有TOPIC，并过滤MM2生成的TOPIC
    '''
    def get_topic_list(self, zkaddress):
        zk_get = "zk.get_children"
        if zkaddress == "10.166.11.51:2181/applogs" or zkaddress == "10.199.135.51:2181/applogs":
            topic = KafkaSyncCheck.connect_zk(zk_get, zkaddress)
            topic_list = [i for i in topic if not i.startswith('mm2-') and not i.startswith('out-') and not i.startswith('open-Rest') and not i.startswith('heartbeats') and not i.startswith('__') and 'checkpoints.internal' not in i]
        elif zkaddress == "10.166.46.12:2181" or zkaddress == "10.199.142.106:2181":
            topic = KafkaSyncCheck.connect_zk(zk_get, zkaddress)
            topic_list = ['binlog']
            for i in topic:
                if i.startswith('cms'):
                    topic_list.append(i)
        else:
            topic = KafkaSyncCheck.connect_zk(zk_get, zkaddress)
            topic_list = [i for i in topic if not i.startswith('mm2-') and not i.startswith('heartbeats') and not i.startswith('__') and 'checkpoints.internal' not in i]
        return topic_list

    '''
    实例方法：对比生产环境和灾备环境TOPIC数量
    '''
    def check_topic(self):
        if self.dre_zkaddress == "10.199.135.51" or self.dre_zkaddress == "10.199.134.51":
            prd_partition_list = self.get_topic_list(self.prd_zkaddress + ":2181" + "/applogs")
            dre_partition_list = self.get_topic_list(self.dre_zkaddress + ":2181" + "/applogs")
            difference_set = set(prd_partition_list).symmetric_difference(set(dre_partition_list))
        elif self.dre_zkaddress == "10.199.134.41" or self.dre_zkaddress == "10.199.134.61":
            prd_partition_list = self.get_topic_list(self.prd_zkaddress + ":2181" + "/kafka")
            dre_partition_list = self.get_topic_list(self.dre_zkaddress + ":2181" + "/kafka")
            difference_set = set(prd_partition_list).symmetric_difference(set(dre_partition_list))
        else:
            prd_partition_list = self.get_topic_list(self.prd_zkaddress + ":2181")
            dre_partition_list = self.get_topic_list(self.dre_zkaddress + ":2181")
            difference_set = set(prd_partition_list).symmetric_difference(set(dre_partition_list))


        if difference_set:
            print("1")
            error_msg = {'message': 'Topic不相同：生产[{}]数量为{}，灾备[{}]数量为{}，差异为{}'.
                format(self.prd_zkaddress + ":2181", len(prd_partition_list), self.dre_zkaddress + ":2181", len(dre_partition_list), difference_set)}
            logger.error(json.dumps(error_msg, ensure_ascii=False))
        else:
            print("0")
            info_msg = {'message': 'Topic相同：生产[{}]数量为{}，灾备[{}]数量为{}'.
                format(self.prd_zkaddress + ":2181", len(prd_partition_list), self.dre_zkaddress + ":2181", len(dre_partition_list))}
            logger.info(json.dumps(info_msg, ensure_ascii=False))



    '''
    实例方法：对比生产环境和灾备环境分区数量
    '''
    def check_topic_partition(self):
        if self.dre_zkaddress == "10.199.135.51" or self.dre_zkaddress == "10.199.134.51":
            prd_partition_list = self.get_topic_list(self.prd_zkaddress + ":2181" + "/applogs")
            prd_partition_dict = KafkaSyncCheck.get_partition_dict(prd_partition_list, self.prd_zkaddress + ":2181" + "/applogs")

            dre_partition_list = self.get_topic_list(self.dre_zkaddress + ":2181" + "/applogs")
            dre_partition_dict = KafkaSyncCheck.get_partition_dict(dre_partition_list, self.dre_zkaddress + ":2181" + "/applogs")
        elif self.dre_zkaddress == "10.199.134.41" or self.dre_zkaddress == "10.199.134.61":
            prd_partition_list = self.get_topic_list(self.prd_zkaddress + ":2181" + "/kafka")
            prd_partition_dict = KafkaSyncCheck.get_partition_dict(prd_partition_list, self.prd_zkaddress + ":2181" + "/kafka")

            dre_partition_list = self.get_topic_list(self.dre_zkaddress + ":2181" + "/kafka")
            dre_partition_dict = KafkaSyncCheck.get_partition_dict(dre_partition_list, self.dre_zkaddress + ":2181" + "/kafka")
        else:
            prd_partition_list = self.get_topic_list(self.prd_zkaddress + ":2181")
            prd_partition_dict = KafkaSyncCheck.get_partition_dict(prd_partition_list, self.prd_zkaddress + ":2181")

            dre_partition_list = self.get_topic_list(self.dre_zkaddress + ":2181")
            dre_partition_dict = KafkaSyncCheck.get_partition_dict(dre_partition_list, self.dre_zkaddress + ":2181")

        diff_set = prd_partition_dict.keys() - dre_partition_dict.keys()
        diff = prd_partition_dict.keys() & dre_partition_dict
        diff_vals = [(k, prd_partition_dict[k], dre_partition_dict[k]) for k in diff if prd_partition_dict[k] != dre_partition_dict[k]]
        if diff_vals or diff_set:
            print("1")
            error_msg = {'message': 'Partition不相同：生产[{}]数量为{}，灾备[{}]数为{}，两边均存在的Topic分区数差异为：{}，两边Topic差集为{}'.
                format(self.prd_zkaddress + ":2181", sum(prd_partition_dict.values()), self.dre_zkaddress + ":2181", sum(dre_partition_dict.values()), diff_vals, diff_set)}
            logger.error(json.dumps(error_msg, ensure_ascii=False))
        else:
            print("0")
            info_msg = {'message': 'Partition相同：生产[{}]数量为{}，灾备[{}]数为{}'.
                format(self.prd_zkaddress + ":2181", sum(prd_partition_dict.values()), self.dre_zkaddress + ":2181", sum(dre_partition_dict.values()))}
            logger.info(json.dumps(info_msg, ensure_ascii=False))



if __name__ == '__main__':
    run_log = RunLog(windows_logfile_dir, linux_logfile_dir, logfile_name)
    logger = run_log.logfile_write()
    if len(sys.argv) == 4:
        prd_zkaddress = sys.argv[1]
        dre_zkaddress = sys.argv[2]
        get_type = sys.argv[3]
        if get_type == "topic_num":
            KafkaSyncCheck(prd_zkaddress, dre_zkaddress).check_topic()
        elif get_type == "partition_num":
            KafkaSyncCheck(prd_zkaddress, dre_zkaddress).check_topic_partition()
        else:
            print("第3个参数输入错误")
    else:
        print("请输入3个参数")