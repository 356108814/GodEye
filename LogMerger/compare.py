# encoding: utf-8
"""

@author Yuriseus
@create 2016-10-21 9:50
"""
filelog_dict = {}
dblog_dict = {}

with open('filelog.txt') as ff:
    for name in ff:
        filelog_dict[name] = 1

with open('dblog.txt') as df:
    for name in df:
        dblog_dict[name] = 1

for name, _ in filelog_dict.items():
    wf = open('result.txt', 'a+')
    if name not in dblog_dict:
        wf.write(name + '\n')