#coding: utf-8

import smtplib
from email.mime.text import MIMEText
from email.header import Header

import random


TEST_MAIL=\
"""
are you ok?
Hello?How old    are you?
ai2yo?
"""

RECEIVER_LIST=["gxm@domain.com",]
TARGET_IP="192.168.1.104"


class CTest(object):

        def Test_Send_Mail_Multi(self, *param):
                print "Test_Send_Mail_Multi ",param

                cnt = 0
                while True:
                        for idx,_server in enumerate( [101,102,103,104] ):
                                #_base_name = ["cs_","pyj_","hqc_","gxm_"][idx]
                                _base_name = ["test_","kk_","gxm_","unknown_"][idx]
                                _base_domain = ["@qq.com","@163.com","@aliyun.com.cn","@youku.com"][idx]

                                sender = "%s%s%s"%(_base_name,idx,_base_domain)
                                receivers = RECEIVER_LIST
                                mail_host=TARGET_IP
                                sText = TEST_MAIL
                                message = MIMEText(sText, 'html', 'utf-8')
                                message['From'] = sender
                                message['To'] =  ",".join(receivers)
                                message['Reply-To']="unknow_helo@notknow.com"
                                port=25
                                #port=10027

                                cnt +=1
                                subject = 'test_mail_%s'%cnt
                                subject=subject.decode("utf-8").encode("gbk")
                                message['Subject'] = Header(subject, 'gbk')

                                code = message.as_string()
                                try:
                                        smtpObj = smtplib.SMTP()
                                        smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
                                        smtpObj.sendmail(sender, receivers, code)
                                        print "%s send mail %s succ"%(sender,str(receivers))
                                except smtplib.SMTPException,err:
                                        print "%s send mail fail: %s"%(sender,str(err))


if __name__ == "__main__":
        import sys
        _obj_test = CTest()
        _obj_test.Test_Send_Mail_Multi()


