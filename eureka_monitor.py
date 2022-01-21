# !/usr/bin/python
# -*- coding:utf-8 -*-
# author: gongxiaoma
# date： 2021-12-31
# version：1.0

import os
import sys
import json
import logging
import logging.config
import platform
import requests


standard_format = '[%(asctime)s][%(threadName)s:%(thread)d][task_id:%(name)s][%(filename)s:%(lineno)d]'\
                  '[%(levelname)s][%(message)s]'
simple_format = '[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d]%(message)s'

windows_logfile_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__))) + '\logs'
linux_logfile_dir = "/tmp"
logfile_name = "eureka_monitor_error.log"


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


class EurekaMonitor(object):
    """
    读取eureka api接口，获取所有服务器实例列表，检查不同环境之间跨环境注册
    """


    """
    类变量：
    指定请求头信息
    """
    header = {'Content-Type': 'application/json', 'Accept': 'application/json'}


    """
    构造函数：
    传入Eureka API地址（获取全局变量）；
    传入IP地址前缀（实例化对象传入）。
    """
    def __init__(self, ip_prefix, eureka_api):
        self.EUREKA_API = eureka_api
        self.IP_PREFIX = ip_prefix


    """
    实例方法：
    获取Eureka所有服务器实例列表，检查是否存在跨区注册（传入IP地址前两位）
    """
    def access_eureka_api(self):
        try:
            result = requests.get(self.EUREKA_API, headers=EurekaMonitor.header).json()
            eureka_application = result["applications"]["application"]
            for i in eureka_application:
                if isinstance(i["instance"], list):
                    for j in i.get("instance"):
                        ip_addr = j.get("ipAddr")
                        if not ip_addr.startswith(self.IP_PREFIX):
                            print("0")
                            sys.exit(1)
                        else:
                            print("1")
                            sys.exit(1)
                else:
                    error_msg = {'status': '失败', 'message': '迭代对象为非列表，请检查：{}'.format(type(i["instance"]))}
                    logger.error(json.dumps(error_msg, ensure_ascii=False))
        except Exception as e:
            error_msg = {'status': '失败', 'message': '获取Eureka所有服务器实例列表或检查异常，请检查：{}'.format(e)}
            logger.error(json.dumps(error_msg, ensure_ascii=False))



if __name__ == '__main__':
    run_log = RunLog(windows_logfile_dir, linux_logfile_dir, logfile_name)
    logger = run_log.logfile_write()
    uat_eureka_api = "http://uat:123456@uat.test.com/eureka/apps"
    prd_eureka_api = "http://prd:123456@uat.test.com/eureka/apps"
    if len(sys.argv) == 2:
        eureka_env = sys.argv[1]
        if eureka_env == "uat":
            ip_prefix = "10.12"
            eureka_monitor = EurekaMonitor(ip_prefix, uat_eureka_api)
            eureka_monitor.access_eureka_api()
        elif eureka_env == "prd":
            ip_prefix = "10.14"
            eureka_monitor = EurekaMonitor(ip_prefix, prd_eureka_api)
            eureka_monitor.access_eureka_api()
        else:
            error_msg = {'status': '失败', 'message': '传入的环境标识错误，请检查：{}'.format(eureka_env)}
            logger.error(json.dumps(error_msg, ensure_ascii=False))
    else:
        print("请输入2个参数")


