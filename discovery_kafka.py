# !/usr/bin/python
# -*- coding:utf-8 -*-

import json
import requests

class DiscoveryKafka(object):
    zabbix_url = "http://zabbix.test.com/api_jsonrpc.php"


    '''
    实例方法：获取zabbix身份验证令牌，由于我已经获取到了，所以后面就不调用该方法
    '''
    def get_zabbix_token(self):
        post_headers = {'Content-Type': 'application/json'}
        post_data = {
            "jsonrpc": "2.0",
            "method": "user.login",
            "params": {
                "user": "Admin",
                "password": "123456"
            },
            "id": 1
        }
        ret = requests.post(DiscoveryKafka.zabbix_url, data=json.dumps(post_data), headers=post_headers)
        print(ret.text)

    '''
    实例方法：
    1、将生产和灾备kafka主机列表文件输出字典和列表
    2、将生产环境主机列表传给zabbix接口获取主机群组和告警人信息
    3、循环zabbix获取的数据，加上对应灾备主机输出zabbix需要的json格式（字段包括：生产主机、灾备主机、告警人1、告警人2）
    '''
    def get_zabbix_user(self):
        kafka_file = open('kafkalist', 'r')
        kafka_dict = {}
        prd_kafka_list = []
        keys = []
        for line in kafka_file:
            v = line.strip().split(':')
            prd_kafka_list.append(v[0])
            kafka_dict[v[0]] = v[1]
            keys.append(v[0])
        kafka_file.close()


        post_headers = {'Content-Type': 'application/json'}
        post_data = {
            "jsonrpc": "2.0",
            "method": "host.get",
            "params": {
                "output": ["host"],
                "selectGroups": "extend",
                "filter": {
                    "host": prd_kafka_list
                }
            },
            "id": 2,
            "auth": "1111111111111111111111111111111"  #获取的身份验证令牌
        }
        post_value = json.dumps(post_data)
        result_text = requests.post(DiscoveryKafka.zabbix_url, data=post_value, headers=post_headers)
        result_text = result_text.content.decode('unicode_escape')
        result_dict = eval(result_text)
        result_list = result_dict["result"]

        data = []
        for i in result_list:
            if i['host'] in kafka_dict:
                dre_host = kafka_dict[i['host']]
                data += [{'{#PRDHOST}': i['host'], '{#DREHOST}': dre_host, '{#USER1}': i['groups'][0]['contactsuser'],
                          '{#USER2}': i['groups'][0]['contactsname']}]
        print(json.dumps({'data': data}, ensure_ascii=False, indent=4))


if __name__ == '__main__':
    DiscoveryKafka().get_zabbix_user()