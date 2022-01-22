#!/usr/bin/env python
#-*- coding:utf-8 -*-
import os
import time
import datetime

class kkmonmail:
    def __init__(self, mailpath):      #构造函数
        self.mailpath = mailpath       #给构造函数赋值
    def delmail(self):                 #定义删除邮件的函数，接收构造函数传过来的参数
        f =  list(os.listdir(self.mailpath))       #列出实例化的路径下面的文件
        print("%s\n  开始清理过期文件...." % self.mailpath)        #打印出要清理的路径，并打印开始清理过期文件
        for i in range(len(f)):            #len统计多少个文件，然后循环赋值给变量i。
            maildate = os.path.getmtime(self.mailpath + f[i])     #获取每个文件的时间的时间，f[i]是获取列表相应下标的值。
            mailtime = datetime.datetime.fromtimestamp(maildate).strftime('%Y-%m-%d')    #格式化时间
            currdate = time.time()         #当前时间
            num1 =(currdate - maildate)/60/60/24      #当前时间减去文件时间，然后换成乘天。 
            if num1 >= 1:
                try:
                    os.remove(self.mailpath + f[i])           #删除符合条件的文件，如果是目录会报错，不会删除
                    print(u"已删除文件：%s ： %s" %  (mailtime, f[i]))
                except Exception as e:
                        print(e)
        else:
            print("......")

maildir = kkmonmail('/tmp2/')       #实例化kkmonmail类
maildir.delmail()        #运行类中的方法
print(u'过期文件已清理完毕：%s\n' % maildir.mailpath)

