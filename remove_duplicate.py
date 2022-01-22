#coding: utf-8

"""
把mailbox进行去重的通用脚本
"""

import argparse
import sys
import time
import os
import os.path
import shutil
import hashlib

import logging
from logging.handlers import TimedRotatingFileHandler

logFilePath = "/root/remove_duplicate.log"
log = logging.getLogger('remove_duplicate')
log.setLevel(logging.DEBUG)
handler = TimedRotatingFileHandler(logFilePath,
               when="d",
               interval=1,
               backupCount=7)
log.addHandler(handler)

#每个目录下的每个MD5值都只会有一个
DUPLICATE_MAIL = {

}

# 取得指定 UID 的用户名，如果未指定 UID 则使用当前运行用户的 UID
def get_system_user_name(uid=None) :
    from pwd import getpwuid
    if uid is None: uid = os.getuid()
    return getpwuid(uid)[0]

# 根据指定的用户名取得对应的 UID
def get_system_user_id(uname) :
    from pwd import getpwnam
    return getpwnam(uname)[2]

# 根据指定的用户名取得对应的 GROUPD
def get_system_group_id(uname) :
    from pwd import getpwnam
    return getpwnam(uname)[3]

def get_md5_value(src):
    myMd5 = hashlib.md5()
    myMd5.update(src)
    myMd5_Digest = myMd5.hexdigest()
    return myMd5_Digest

# 递归创建路径
def recursion_make_dir(path, permit=0755) :
    path = path.replace("\\","/")
    if os.path.exists(path) : return False
    os.makedirs(path)
    os.chmod(path, permit)
    return True

def scan_dir_files( path, filter_func = None ):
    for root, dirs, files in os.walk(path):
        for file_name in files:
            if filter_func and not filter_func( root, file_name ):
                continue
            yield ( root, file_name )

#初始化
def init_argument_parser() :
        # 创建解析器
        parser = argparse.ArgumentParser(
        prog='remove_duplicate',
        formatter_class=argparse.RawTextHelpFormatter,
        description='''
prepare_patch
Command                             Description
--------------------------------------------------------------------

'''
        )
        #必选参数
        parser.add_argument(
                'mailbox_root',
                metavar='param_name',
                nargs='?',
                )
        parser.add_argument(
                'backup_root',
                metavar='param_value',
                nargs='?',
                )
        parser.add_argument(
                'discard_root',
                metavar='param_value',
                nargs='?',
                )
        return parser

def notify(msg):
    time_info=time.strftime('%Y-%m-%d %H:%M:%S')
    msg="[%s] %s"%(time_info,msg)
    print msg
    log.info(msg)

def main( obj_args ) :
    def strip_code(code):
            code = code.strip("\r\n")
            code = code.strip("\r")
            code = code.strip("\n")
            return code.strip()

    mailbox_root = strip_code(obj_args.mailbox_root)
    backup_root = strip_code(obj_args.backup_root)
    discard_root = strip_code(obj_args.discard_root)

    if not mailbox_root or not backup_root or not discard_root:
        print "未输入足够参数！"
        sys.exit(1)

    if not os.path.exists(mailbox_root):
        print "源目录%s 不存在！"%mailbox_root
        sys.exit(2)
    if not os.path.exists(backup_root):
        print "目标目录%s 不存在！"%backup_root
        sys.exit(2)
    if not os.path.exists(discard_root):
        print "备份目录%s 不存在！"%discard_root
        sys.exit(2)

    notify("扫描根目录%s"%mailbox_root)
    for subdir in os.listdir(mailbox_root):
	continue
        dirpath = "%s/%s"%(mailbox_root, subdir)
        if not os.path.isdir(dirpath):
            continue
        notify("扫描目录%s"%subdir)
        for root, filename in scan_dir_files(dirpath):
            try:
                if filename.startswith("dovecot"):
                    continue

                filepath = "%s/%s"%(root, filename)
                dirname = os.path.dirname(filepath)

                filepath2 = filepath.replace(mailbox_root, backup_root)
                dirname2 = os.path.dirname(filepath2)
                recursion_make_dir(dirname2)

                filepath3 = filepath.replace(mailbox_root, discard_root)
                dirname3 = os.path.dirname(filepath3)
                recursion_make_dir(dirname3)

                with open(filepath,"rb") as fobj:
                    md5 = get_md5_value(fobj.read())
                DUPLICATE_MAIL.setdefault( dirname, {} )
                if not md5 in DUPLICATE_MAIL[dirname]:
                    notify("将 %s 迁移到 %s md5: %s"%(filepath, filepath2, md5))
                    DUPLICATE_MAIL[dirname][md5] = filepath
                    shutil.move(filepath, filepath2)
                else:
                    lastfile = DUPLICATE_MAIL[dirname][md5]
                    notify("忽略文件 %s ， 与上一个文件 %s 冲突了"%(filepath, lastfile))
                    shutil.move(filepath, filepath3)
            except Exception,err:
                notify("扫描%s/%s发生错误:%s，忽略"%(root,filename,str(err)))

    notify("开始还原邮件到%s"%mailbox_root)
    for subdir in os.listdir(backup_root):
        dirpath = "%s/%s"%(backup_root, subdir)
        if not os.path.isdir(dirpath):
            continue
        notify("扫描目录%s"%subdir)
        for root, filename in scan_dir_files(dirpath):
            filepath = "%s/%s"%(root, filename)
            filepath2 = filepath.replace(backup_root, mailbox_root)
            notify("将 %s 恢复到 %s"%(filepath, filepath2))
            shutil.move(filepath, filepath2)
            os.chown(filepath2, get_system_user_id("kk"), get_system_group_id("kk") )

    notify("开始完毕，请确认数据无误后手动删除 %s"%discard_root)
    notify("请检查%s目录，若发现有root权限的文件，请执行 chown -R kk:kk %s"%(mailbox_root,mailbox_root))


if __name__ == "__main__":
    help_code = """
    python remove_duplicate.py 邮件目录 目标目录 备份丢弃目录

    执行进程后会将"邮件目录"中的内容迁移到"目标目录"，迁移的过程中会根据MD5值去掉重复邮件。
    迁移完毕后，会将已去重的"目标目录"中的邮件重新迁移回"邮件目录"中。
    为了防止操作失误，操作前需要手动创建相关目录。
    丢弃的邮件会先存在"备份丢弃目录" 目录下，需要手动删除。
    log记录在 /root/remove_duplicate.log。
    执行原理：
    先将"邮件目录"里的所有邮件移动到"目标目录"下，MD5值相同的文件移动到"备份丢弃目录"。
    移动完毕后，再将"目标目录"里的所有邮件恢复到"邮件目录"中。
    >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n\n
    """
    print help_code

    parser = init_argument_parser()
    obj_args   = parser.parse_args()
    main( obj_args )
