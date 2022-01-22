#!/usr/bin/python
#coding=utf-8
 
import urllib
import urllib2
import json
import sys
import re
 
headers = {'Content-Type': 'application/json'}
 
test_data = {
    'msgtype':"text",
    "text":{
        'content':"%s" % sys.argv[1]
    }, 
    "at":{
        "atMobiles":[
            手机号码1,
            手机号码2
        ], 
        "isAtAll":False
    }
}
 
requrl = "钉钉机器人的提交地址"
req = urllib2.Request(url = requrl,headers = headers,data = json.dumps(test_data))
response = urllib2.urlopen(req)

