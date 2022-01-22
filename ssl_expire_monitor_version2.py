#/usr/bin/python
#-*- coding: utf-8 -*-

import os,sys  
import zipfile 
import datetime
import subprocess
import smtplib
import shutil
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr

ssl_zip_dir='/root/ssl'
ssl_save_dir='/tmp/ssl'



#SMTP相关信息
mail_host="mail.xxx.com"
mail_user="gxm@xxx.com"
mail_pass="123456" 
sender = 'gxm@xxx.com'
receivers = ['emma@xxx.com','gxm@xxx.com']


#定义发件人、收件人、标题
#','.join(receivers)表示连接多个收件人
def msg(content):
    message = MIMEText(content,'plain','utf-8')
    message['From'] = formataddr((Header('SSL通知邮箱', 'utf-8').encode(), mail_user))
    message['To'] =  ','.join(receivers)
    message['Subject'] = Header('SSL证书即将到期提醒','utf-8')
    return message



def send_mail(message):
    try:
        smtpObj = smtplib.SMTP() 
        smtpObj.connect(mail_host, 25)
        smtpObj.login(mail_user,mail_pass)  
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")



#批量解压zip压缩包到“/tmp/ssl/压缩包名前缀”目录下，然后用openssl查看证书到期时间和域名，将最近30天到期的证书发送邮件提醒下
#subprocess.check_output这个是获取shell命令运行后通过管道传输回来数据，如果用os.system只是输出而已
#startswith以什么开始
#os.path.join组合路径
#decode('utf-8').strip()表示将字节转换成str类型、和将前后空格、换行去掉等
#时间可以与时间字节对比，只要格式一样
#timedelta(30)表示间隔30天的意思
#msg(cont)表示调用msg函数，并且将cont传递给msg里面定义为的正文信息
#send_mail(m)表示调用send_mail函数
def un_zip(files,root_dirs):
    os.chdir(root_dirs)#转到路径
    for file_name in files:
        r = zipfile.is_zipfile(file_name)#判断是否解压文件
        if r:
            zpfd = zipfile.ZipFile(file_name)#读取压缩文件
            for file in zpfd.namelist():
                if file.startswith('Server'):
                    dir = os.path.join('/tmp/ssl', file_name[:-4])
                    zpfd.extract(file, dir)
                    file_cer = os.path.join(dir,'ServerCertificate.cer')
                    domain = subprocess.check_output("openssl x509 -text -noout -in {} | grep 'DNS:' |awk -F',' '{{print $1}}' | awk -F'DNS:' '{{print $2}}'".format(file_cer), shell=True)
                    domain = domain.decode('utf-8').strip()
                    after_date = subprocess.check_output("openssl x509 -text -noout -in {} | grep 'Not After' |awk -F' GMT' '{{print $1}}' |awk -F'Not After : ' '{{print $2}}'".format(file_cer), shell=True)
                    after_date = after_date.decode('utf-8').strip()
                    t0 = datetime.datetime.strptime(after_date, '%b %d %H:%M:%S %Y')
                    t1 = datetime.datetime.now()
                    if t1 <= t0 <= t1 + datetime.timedelta(30):
                        expire_soon = '证书即将到期（提醒30天之内的） 域名：{} 到期时间： {}'.format(domain, t0)
                        mailcontent = msg(expire_soon)
                        send_mail(mailcontent)
            zpfd.close()



#获取压缩包目录下所有文件，并打印出文件路径、子目录路径和文件名
#将文件、文件路径作为参数传递给un_zip函数
def ssl_zip(ssl_zip_dir):
    for root_dirs,sub_dirs,files in os.walk(ssl_zip_dir):
#        print(root_dirs,sub_dirs,files)
#        print(files)
        un_zip(files,root_dirs)



if __name__=='__main__':
    #如果ssl解压后的目录不存在，先新建
    if not os.path.exists(ssl_save_dir):
        os.makedirs(ssl_save_dir)
    ssl_zip(ssl_zip_dir)

