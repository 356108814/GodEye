# encoding: utf-8
"""

@author Yuriseus
@create 2016-9-30 18:14
"""
import time

from pyhdfs import HdfsClient
client = HdfsClient(hosts='172.16.2.190:50070, 172.16.2.191:50070', user_name='hdfs')
# print(client.create('/user/logs/1.log', ''))
index = 1
# while True:
#     with client.open('/user/logs/1.log') as f:
#         f.write('111')
client.append('/user/logs/1.log', 'asdfaskfkaksdfk')
# client.append('/user/logs/1.log', '222222')
    # index += 1
    # time.sleep(0.5)
