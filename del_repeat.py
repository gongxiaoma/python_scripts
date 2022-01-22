#!/usr/bin/env python
#coding: utf-8
import re
import os
import shutil
import time
import random
import sys
import urllib
import hashlib
#重复邮件的目录
FROM_FOLDER_LIST=["/tmp",]
#准确的，即去重后的需要的邮件
TARGET_FOLDER="/root/patch1"
#得到的重复邮件
TARGET_FOLDER2="/root/patch3"
def get_md5_value(src):
        #调用hashlib里的md5()生成一个md5 hash对象
        myMd5 = hashlib.md5()
        #生成hash对象后，就可以用update方法对字符串进行md5加密的更新处理
        myMd5.update(src)
        #加密后的十六进制结果
        myMd5_Digest = myMd5.hexdigest()
        #返回十六进制结果
        return myMd5_Digest


MD5_POOL={
}

for FROM_FOLDER in FROM_FOLDER_LIST:
        for root, dirs, files in os.walk(FROM_FOLDER):
                for file_name in files:
                        filepath = "%s/%s"%(root,file_name)
                        print "扫描文件  ",filepath
                        #以上几行是循环扫描每个文件，并打印出来
                        try:
                                #打开一个文件，并定义为fobj别名
                                with open(filepath,"rb") as fobj:
                                        #读文件
                                        code = fobj.read()
                                #调用获取md5值的函数，返回文件的十六进制结果，并赋值给md5_v变量
                                md5_v = get_md5_value(code)
                                #假如md5_v没在MD5_POOL池（字典）中，然后打印出来，并移动到TARGET_FOLDER目录
                                if not md5_v in MD5_POOL:
                                        print "移动文件 %s 到目录 %s下"%(filepath,TARGET_FOLDER)
                                        shutil.move(filepath, "%s/%s"%(TARGET_FOLDER,file_name))
                                        #然后加到池中，一个是key，一个是值
                                        MD5_POOL[md5_v] = file_name
                                else:
                                        #假如池中，就重复了，就移动到TARGET_FOLDER2目录
                                        print "%s 是重复文件， 移动到目录 %s下"%(filepath,TARGET_FOLDER2)
                                        shutil.move(filepath, "%s/%s"%(TARGET_FOLDER2,file_name))
                        except Exception,err:
                                print err
                                print "copy file %s error"%filepath
                                continue
