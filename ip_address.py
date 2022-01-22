#!/usr/bin/python
#coding:utf-8
import urllib2
import json

ip=raw_input("请输入要查询的IP:")
def ip_address(ip):
    apiurl = "http://ip.taobao.com/service/getIpInfo.php?ip=%s" % ip
    content = urllib2.urlopen(apiurl).read()
    data = json.loads(content)['data']
    code = json.loads(content)['code']
    if code == 0:
        print "\nIP: %s Form: %s%s%s ISP: %s\n" % (data['ip'], data['country'], data['region'], data['city'], data['isp'])
    else:
        print data

ip_address(ip)

