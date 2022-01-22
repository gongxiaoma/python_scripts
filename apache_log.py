#!/usr/bin/python
# coding: utf-8
import os
import re
import pprint
import datetime
import urllib2
import json
import sys


if len(sys.argv) == 0:
    print 'usage: python 脚本文件 日志文件'
file1 = sys.argv[1]


if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')


def ip_into_int(ip):
    return reduce(lambda x,y:(x<<8)+y,map(int,ip.split('.')))


def is_internal_ip(ip):
    ip = ip_into_int(ip)
    net_a = ip_into_int('10.255.255.255') >> 24
    net_b = ip_into_int('172.31.255.255') >> 20
    net_c = ip_into_int('192.168.255.255') >> 16
    return ip >> 24 == net_a or ip >>20 == net_b or ip >> 16 == net_c


def count(ip_list):
    iplist = {}
    for ip in ip_list:
        if ip not in iplist:
            iplist[ip] = 0
        iplist[ip] += 1
    l = list(iplist.items())
    l.sort(key=lambda x:x[1], reverse=True)
    return l


def ip_address(ip):
    apiurl = "http://ip.taobao.com/service/getIpInfo.php?ip=%s" % ip
    content = urllib2.urlopen(apiurl).read()
    data = json.loads(content)['data']
    code = json.loads(content)['code']
    if is_internal_ip(ip):
        dili = "%s" % (data['city'])
        return dili
    if code == 0:
        dili = "%s%s%s" % (data['country'], data['region'], data['city'])
        return dili


d = {}
with open(file1) as f:
    for line in f:                                                                          
        m = re.search(r'([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}) - - \[([0-9]{1,2}\/[A-Za-z]{3}\/[0-9]{4})', line.strip())
        if m is not None:
            ip, date_dmy = m.groups()
            date = datetime.datetime.strptime(date_dmy, '%d/%b/%Y')
            if date not in d:
               d[date] = {
                  'ip': []
                }
            d[date]['ip'].append(ip)


l = list(d.items())
l.sort(key=lambda x: x[0])
for date_dmy,e in l:
    print '\n'
    print '%s%s%s' %('*'*20,date_dmy.strftime('%Y-%m-%d'),'*'*20)
    l = count(e['ip'])
    for ip,num in l[:20]:
        print 'IP为 %s，访问次数为 %d, IP所在地区为%s' % (ip,num,ip_address(ip))
        #python3版本打印为---print('IP为 {}，访问次数为 {}'.format(ip,num))
