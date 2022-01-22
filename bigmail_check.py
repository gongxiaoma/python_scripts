#!/usr/bin/python
# coding: utf-8
import sys
import re
import pprint


if len(sys.argv) == 0:
    print 'usage: python 脚本文件 日志文件'
file1 = sys.argv[1]


d = {}

with open(file1) as f:
    for line in f:
        m = re.search(r'\[([0-9A-Za-z-]{21})\].*size: ([0-9]+)$', line.strip())
        if m is not None:
            id, size = m.groups()
            if id not in d:
                d[id] = {
                  'size': 0,
                  'recp': []
                }
            d[id]['size'] = int(size)

        m = re.search(r'\[([0-9A-Za-z-]{21})\] save file to:.*\(([^\(\)]+)\).*', line.strip())
        if m is not None:
            id, recp = m.groups()
            if id not in d:
                d[id] = {
                  'size': 0,
                  'recp': []
                }
            d[id]['recp'].append(recp)


l = []
for id, e in d.items():
    total_size = e['size'] * len(e['recp']) / (1024.0 ** 2)
    l.append((id, len(e['recp']), total_size))

print '\n'
print '>>>>>>>>>>>>>>>统计每个ID产生的收件人数量和总容量（TOP20）>>>>>>>>>>>>>>>'
l.sort(key=lambda x: x[2], reverse=True)
for id, recp_num, total_size in l[:20]:
    print '邮件id为%s，收件人数量为%d个，总大小为%dM' % (id,recp_num,total_size)

