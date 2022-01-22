#coding: utf-8

import re
import os
import shutil
import time
import random
import sys
import urllib
import email
import email.header
import chardet
from tempfile import TemporaryFile

re_FindEmailAddr=re.compile("\<([\w\d\-\_]+\@xinquan\.cn)\>")

ALL_MAIL_MAP = {}

# 随机字符生成种子
SEEDS  = [ chr(i) for i in range(48,  58) ]
SEEDS += [ chr(i) for i in range(65,  91) ]
SEEDS += [ chr(i) for i in range(97, 123) ]

# 生成随机字符串
def get_random_string(str_len=5) :
    return ''.join(random.sample(SEEDS, str_len))

# 从完整的任务ID中解析出任务主ID
def parse_task_main_id(task_id) :
    return task_id.split('-')[0]

# 生成完整的任务ID
def generate_task_id(main_id = None, sub_id = None) :
    if main_id is None : main_id = generate_task_main_id()
    if sub_id  is None : sub_id  = generate_task_sub_id()
    task_id = main_id + '-' + sub_id
    return task_id

# 生成任务主ID
def generate_task_main_id() :
    main_id = str(time.time())[:10] + get_random_string(5)
    return main_id

# 生成任务子ID
def generate_task_sub_id() :
    return get_random_string(5)

# 创建指定文件的副本
def create_file_copy(src_file, main_id = None, message='') :
    savepath = os.path.dirname(src_file)
    task_id  = generate_task_id(main_id)
    dst_file = os.path.join(savepath, task_id)
    if message:
        with open(dst_file, 'w') as fw:
            fw.write(message)
    else:
        shutil.copy(src_file, dst_file)
    return task_id, dst_file

# 转换字符串为 unicode
def get_unicode(string) :
    st = type(string)
    if st.__name__ == 'unicode' :
        return string
    charset = chardet.detect(string)['encoding']
    return string.decode(charset)

def get_string(code):
    if isinstance(code,unicode):
        return code.encode('utf-8','ignore')
    return str(code)

# 解码邮件主题
def decode_header_string(raw_string) :
    """
    :param raw_string:
    :return: string(UTF编码的字符串), charset(原始编码）
    """
    if raw_string is None : return "", 'utf-8'
    raw_string = raw_string.replace('\n', '')

    # 对原始字符串进行兼容性处理，此处理只能针对 BASE64 编码，不能对 QUOTED-PRINTABLE
    # 编码进行操作
    if ('?B?' in raw_string or '?b?' in raw_string) and ('?==?' in raw_string):
        for _item in re.findall(r"(=\?.*?\?=)\S", raw_string) :
            raw_string = raw_string.replace(_item, _item + ' ')

    # 对邮件主题进行初步解码
    try    : parts = email.header.decode_header(raw_string)
    except Exception,e:
        print "error!!!!! ",e
        return raw_string, 'utf-8'

    # 处理邮件主题
    full_str = ''
    charset  = None

    for part in parts :
        if charset : charset=charset.lower()

        # 判断当前部分的字符编码
        if part[1] :
            _charset = 'gbk' if part[1] == 'gb2312' else part[1]
            if part[1] == '136' : _charset = 'ascii'
        else :
            _charset = chardet.detect(part[0])['encoding']
        if charset is None : charset = _charset

        if _charset in ['default', 'us-ascii', 'gb2312', 'iso-2022-cn', 'windows-936', 'gb_1988-80', 'windows-1252']:
            _charset = 'gbk'

        # 将当前部分追加至主题中
        if   _charset is None :
            try:
                part_str = part[0].encode('gbk', 'ignore')
            except UnicodeError, e:
                try:
                    part_str = part[0].encode('utf-8', 'ignore')
                except:
                    part_str = part[0].decode('gbk', 'ignore').encode('utf-8', 'ignore')
        elif _charset == 'utf-8' :
            part_str = part[0]
        else :
            try: part_str = part[0].decode(_charset, 'ignore').encode('utf-8', 'ignore')
            except: part_str = part[0].decode('gbk', 'ignore').encode('utf-8', 'ignore')
        full_str += ' ' + part_str
    return full_str.lstrip(), charset


# 取得指定头解码后的内容
def get_header_value(header, field, default=None) :
    field = field.lower()

    # subject
    if   field in ['subject'] :
        raw_value = header.get(field, None)
        if raw_value is None : return default
        return decode_header_string(raw_value)[0]

    # from, sender
    elif field in ['from', 'sender'] :

        # 取得原始字符串
        raw_value = header.get(field, None)
        if raw_value is None : return default

        # 处理地址
        addresses = list(email.utils.parseaddr(raw_value))
        addresses[0] = decode_header_string(addresses[0])[0]
        addresses[1] = '<%s>' % addresses[1]
        return addresses[1] if addresses[0] == '' else ' '.join(addresses)

    # to, cc
    elif field in ['to', 'cc', 'bcc'] :

        # 取得原始数据列表
        raw_value = header.get_all(field, None)
        if raw_value is None : return default

        # 处理地址列表
        raw_list = email.utils.getaddresses(raw_value)
        new_list = []
        for addresses in raw_list :
            addresses    = list(addresses)
            addresses[0] = decode_header_string(addresses[0])[0]
            addresses[1] = '<%s>' % addresses[1]
            new_list.append(addresses[1] if addresses[0] == '' else ' '.join(addresses))
        return ', '.join(new_list)

    # date
    elif field == 'date' :
        _date = header.get('Date', None)
        if _date is None : return default
        _date = _date.decode('latin-1').encode("utf-8")
        return _date
    elif field == 'datetime' :
        try:
            _date = header.get('Date', None)
            if _date is None : return default
            date_ = email.utils.parsedate(_date)
            ts = calendar.timegm(date_)
            ts = datetime.datetime.utcfromtimestamp(ts)
            now = datetime.datetime.now()
            if ts > now:
                ts = now
            return ts.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return default

    # 其它
    return header.get(field, default)

# 取得指定邮件的头信息 (邮件文件)
def get_mail_header_by_file(mailpath) :
    lines = []
    fp = open(mailpath, 'r')
    while True :
        line = fp.readline()
        if line.rstrip() == '' :
            fp.close()
            break
        lines.append(line)
    return ''.join(lines)


TARGET_DOMAIN="xinquan.cn"


def check_recover_single_mail(message_id, mailbox,mail_file,rcv_type="send"):
        global TOTAL_SCAN_MAIL
        global TOTAL_SAVE_MAIL

        mailbox = mailbox.strip()
        mailbox = mailbox.replace("'","")
        mailbox = mailbox.replace('"',"")
        mailbox = mailbox.replace(' ',"")
        box_robj = re_FindEmailAddr.search( mailbox )
        if not box_robj:
                return
        mailbox = box_robj.group(1)
        print "check_recover_mail  ",mail_file,mailbox
        if not "@%s"%TARGET_DOMAIN in mailbox:
                return
        ALL_MAIL_MAP.setdefault( mailbox, {})
        if message_id:
                if message_id in ALL_MAIL_MAP[mailbox]:
                        print "skip %s by message is same %s"%(mail_file,message_id)
                        return
                ALL_MAIL_MAP[mailbox][message_id] = mail_file

        mailbox = get_string( mailbox )
        mail_file = get_string( mail_file )

        save_dir = TARGET_DIR
        if not save_dir in ALL_DIRS:
            ALL_DIRS[save_dir] =0
        idx = 0
        while ALL_DIRS[save_dir] >= 20000:
            idx += 1
            save_dir = "%s_%s"%(TARGET_DIR,idx)
            if not save_dir in ALL_DIRS:
                ALL_DIRS[save_dir] =0

        box_name = mailbox.split("@")[0]
        save_path = "%s/%s/%s"%(save_dir,TARGET_DOMAIN,box_name)

        for dir_name in ["cur","new","tmp",".Drafts",".Sent",".Sent/cur",".Sent/new",".Sent/tmp",".Trash",".Spam"]:
                dir_name = "%s/%s"%(save_path,dir_name)
                if not os.path.exists( dir_name ):
                    try:
                        os.makedirs( dir_name )
                    except Exception,err:
                        with open("%s/make_dir_err.log"%SCAN_DIR,"a+") as fobj_err:
                            code = "make file %s dir %s err: \t\t%s\n"%( mail_file, dir_name, str(err) )
                            print code
                            fobj_err.write( code )
                        continue


        if rcv_type == "send":
                file_path = "%s/.Sent/cur"%save_path
        else:
                file_path = "%s/cur"%save_path

        task_id = generate_task_id( )
        copy_file = "%s-%s"%(task_id,mailbox)
        copy_file = "%s/%s"%(file_path,copy_file)
        print "copy file %s to %s maibox save at: %s"%(mail_file,mailbox,copy_file)
        try:
            shutil.copy(mail_file, copy_file)
        except Exception,err:
            with open("%s/copy_mail_err.log"%SCAN_DIR,"a+") as fobj_err:
                code = "copy file %s -> %s err: \t\t%s\n"%( mail_file, copy_file, str(err) )
                print code
                fobj_err.write( code )
            return ""

        TOTAL_SAVE_MAIL += 1
        ALL_DIRS[save_dir]+=1

        with open("%s/copy_mail.log"%SCAN_DIR,"a+") as fobj_log:
            code = "copy %s -> %s\n"%( mail_file, copy_file )
            fobj_log.write( code )
        return copy_file


def scan_dir(task_dir):
        global TOTAL_SCAN_MAIL
        global TOTAL_SAVE_MAIL
        if not os.path.exists( TARGET_DIR ):
                os.mkdir( TARGET_DIR )
        if not os.path.exists( "%s/%s"%(TARGET_DIR,TARGET_DOMAIN) ):
                os.mkdir( "%s/%s"%(TARGET_DIR,TARGET_DOMAIN) )

        for root, dirs, files in os.walk(task_dir):
                for file_name in files:
                        filepath = "%s/%s"%(root,file_name)
                        mail_obj = email.message_from_string(get_mail_header_by_file(filepath))

                        try:
                                sender = get_header_value(mail_obj, 'From', '')
                                recipients = get_header_value(mail_obj, 'To', '')
                                message_id = get_string(get_header_value(mail_obj, 'Message-ID', ''))
                        except:
                                print "decode file %s error"%filepath
                                continue
                        if not sender and not recipients:
                                continue
                        sender = get_string( sender )
                        recipients = get_string( recipients )
                        recipient_list = recipients.split(",")

                        TOTAL_SCAN_MAIL += 1
                        check_recover_single_mail( message_id, sender , filepath, "send" )
                        for mailbox in recipient_list:
                                check_recover_single_mail( message_id, mailbox, filepath, "recv" )
        print "all done!!!"
        print "total scan: %s "%TOTAL_SCAN_MAIL
        print "total save: %s "%TOTAL_SAVE_MAIL


if __name__ == "__main__":
        help_code = """
<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
example:
将程序文件放到需要恢复的文件夹所在目录，例如：恢复目录为 example
然后执行：
/usr/local/kkmail/app/engine/bin/python recover_mail.py example example2
分类好的邮件会存放于 example2 下面

执行完后请执行 chown -R kkmail:kkmail example2
        """
        print help_code

        TOTAL_SCAN_MAIL = 0
        TOTAL_SAVE_MAIL = 0

        task_dir = sys.argv[1]
        SCAN_DIR = task_dir
        TARGET_DIR = sys.argv[2]
        ALL_DIRS = {}
        print "scan: %s"%task_dir
        scan_dir( task_dir )

