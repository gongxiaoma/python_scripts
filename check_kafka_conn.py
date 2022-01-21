# !/usr/bin/python
# -*- coding:utf-8 -*-
# author: gongxiaoma
# date： 2021-12-16
# version：1.0
# zkpython最晚更新时间2012年，kazoo去年还有更新。
import os
import re
import sys
import json
import logging
import logging.config
import platform
import requests
from kafka import KafkaAdminClient, errors


standard_format = '[%(asctime)s][%(levelname)s][%(message)s]'
simple_format = '[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d]%(message)s'

windows_logfile_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__))) + '\logs'
linux_logfile_dir = "/tmp"
logfile_name = "check_kafka-error.log"


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

class CheckKafka(object):

    def __init__(self, project):
        '''
        构造函数：
        脚本参数传入项目名
        :param project:
        '''
        self.project = project


    def download_kafka_file(self):
        '''
        实例方法：
        从gitlab上下载kafka列表文件，并且保存在本地
        :return:
        '''
        try:
            result = requests.get('http://soft.test.com/dre/kafka_list.txt')
            if result.status_code == 200:
                os = platform.system()
                if os == "Windows":
                    f = open("kafka_list.txt", "wb")
                    for chunk in result.iter_content(chunk_size=512):
                        if chunk:
                            f.write(chunk)
                    f.close()
                else:
                    f = open("/tmp/kafka_list.txt", "wb")
                    for chunk in result.iter_content(chunk_size=512):
                        if chunk:
                            f.write(chunk)
                    f.close()
            else:
                error_msg = {'status': '失败', 'message': 'gitlab下载链接错误，请检查：{}'.format(result.status_code)}
                logger.error(json.dumps(error_msg, ensure_ascii=False))
                sys.exit(1)
        except Exception as e:
            error_msg = {'status': '失败', 'message': '从gitlab上下载kafka列表文件发生异常，请检查：{}'.format(e)}
            logger.error(json.dumps(error_msg, ensure_ascii=False))
            raise e


    def format_kafka_file(self):
        '''
        实例方法：
        将kafka列表文件转换为字典，部分格式错误会抓到异常输出错误到日志
        :return:
        '''
        try:
            os = platform.system()
            if os == "Windows":
                kafka_file = open('kafka_list.txt', 'r')
            else:
                kafka_file = open('/tmp/kafka_list.txt', 'r')
            kafka_dict = {}
            kafka_list = []
            keys = []
            for line in kafka_file:
                v = line.strip().split(':')
                kafka_list.append(v[0])
                kafka_dict[v[0]] = v[1]
                keys.append(v[0])
            kafka_file.close()
            return kafka_dict
        except Exception as e:
            error_msg = {'status': '失败', 'message': 'kafka列表文件格式发生错误，请检查：{}'.format(e)}
            logger.error(json.dumps(error_msg, ensure_ascii=False))
            raise e


    def check_project_ip(self):
        '''
        实例方法：
        检查传入的项目名是否存在；
        检查IP格式是否正确；
        如果IP不存在返回1，项目不存在返回2，供调用方法获取
        :return:
        '''
        try:
            ip_format = re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
            self.download_kafka_file()
            kafka_dict = self.format_kafka_file()
            if project in kafka_dict.keys():
                ip_list = eval(kafka_dict[project])
                for ip in ip_list:
                    if not ip_format.match(ip):
                        error_msg = {'status': '失败', 'message': 'gitlab上kafka列表文件ip格式不对，请检查：{}'.format(ip)}
                        logger.error(json.dumps(error_msg, ensure_ascii=False))
                        return 1
                return ip_list
            else:
                error_msg = {'status': '失败', 'message': 'gitlab上kafka列表文件不存在该项目，请检查：{}'.format(project)}
                logger.error(json.dumps(error_msg, ensure_ascii=False))
                return 1
        except Exception as e:
            error_msg = {'status': '失败', 'message': 'gitlab上kafka列表文件发生错误，请检查：{}'.format(e)}
            logger.error(json.dumps(error_msg, ensure_ascii=False))
            raise e


    def export_broker_ip(self):
        '''
        实例方法：
        调用check_project_ip方法，获取返回的值。
        如果接收到的是一个列表（正确的IP列表），如果是1表示IP格式不对，如果是2表示项目名不对；
        接收到IP列表后，检查所有broker是否可以连接成功，只要有一个连接不成功就会中断程序，并且输出提示和日志。
        :return:
        '''
        result = self.check_project_ip()
        if isinstance(result, list):
            for ip in result:
                try:
                    connect = KafkaAdminClient(bootstrap_servers=ip)
                except errors.NoBrokersAvailable:
                    error_msg = {'status': '失败', 'message': '连接kafka Broker节点异常，请检查网络和进程：{}'.format(ip)}
                    logger.error(json.dumps(error_msg, ensure_ascii=False))
                    print("异常：连接Kafka Broker节点异常，请检查网络和进程")
                    sys.exit(1)
                except Exception as e:
                    error_msg = {'status': '失败', 'message': '连接kafka Broker节点异常，请检查：{}'.format(e)}
                    logger.error(json.dumps(error_msg, ensure_ascii=False))
                    print("异常：连接Kafka Broker节点异常，请检查日志")
                    sys.exit(1)
            return result
        else:
            return 1


    def check_broker_connect(self):
        '''
        实例方法：
        调用export_broker_ip方法，获取返回的值。如果接收到的是一个列表（正确的IP列表），如果是非列表则输出异常；
        :return:
        '''
        result = self.export_broker_ip()
        if isinstance(result, list):
            print("正常")
        else:
            print("异常：详细错误请查看/tmp/check_kafka-error.log日志")


    def check_topic_metadata(self):
        '''
        实例方法：
        调用export_broker_ip方法，获取返回的值（正确的IP列表）；
        使用第一个IP连接获取topic元数据（包括topic名称、分区和leader等）；
        判断所有分区leader是否正常，只要有一个分区leader异常就会中断程序，并且输出提示和日志。
        :return:
        '''
        result = self.export_broker_ip()
        if isinstance(result, list):
            frist_ip = result[0]
            connect = KafkaAdminClient(bootstrap_servers=frist_ip)
            topic_list = connect.list_topics()
            topic_describe = connect.describe_topics(topic_list)
            for topic in topic_describe:
                for p in topic['partitions']:
                    if p.get('leader') in [-1, "none", None]:
                        print("异常: " + topic['topic'] + "分区leader状态异常")
                        error_msg = {'status': '失败', 'message': '{}等分区leader状态异常，请检查：'.format(topic['topic'])}
                        logger.error(json.dumps(error_msg, ensure_ascii=False))
                        sys.exit(1)
            print("正常")
        else:
            print("异常：详细错误请查看/tmp/check_kafka-error.log日志")



if __name__ == '__main__':
    run_log = RunLog(windows_logfile_dir, linux_logfile_dir, logfile_name)
    logger = run_log.logfile_write()
    if len(sys.argv) == 3:
        project = sys.argv[1]
        check_type = sys.argv[2]
        if check_type == "broker":
            CheckKafka(project).check_broker_connect()
        elif check_type == "topic":
            CheckKafka(project).check_topic_metadata()
        else:
            print("第2个参数请输入broker或topic")
    else:
        print("请输入2个参数")
