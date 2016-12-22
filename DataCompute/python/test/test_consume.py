# encoding: utf-8
"""

@author Yuriseus
@create 16-11-3 下午1:49
"""
import json

import pickle

from config.enums import WEEKDAY
from util.date import DateUtil


def process_data(d):
    """
    处理data数据，分解data字典，去除空数据
    :param d:
    :return:
    """
    data = d['data']
    del d['data']
    for k, v in data.items():
        if v == '' or (isinstance(v, list) and not v):
            continue
        d[k] = v

    if 'uid' not in data:
        d['uid'] = 0
    # 增加登陆日期
    timestamp = int(d['timestamp'])
    d['date'] = DateUtil.timestamp2date(timestamp, '%Y-%m-%d %H:%M:%S')
    d['hour'] = d['date'][0:13]  # 用于按小时统计
    return d


if __name__ == '__main__':
    path = '/opt/data-log/minik/2016-10-20.log'
    count = 0
    ds = []
    with open(path, 'r') as f:
        for line in f:
            d = {}
            tmp = line.split('&')
            d['timestamp'] = tmp[0]
            d['mid'] = tmp[1]
            d['interface'] = tmp[2]
            try:
                # 因为有异常情况导致解析的日志中的格式是错误的
                d['data'] = json.loads(tmp[3])  # 字典
            except Exception:
                d['data'] = None
            d['exec_version'] = tmp[4]  # 程序版本
            d['ui_version'] = tmp[5]  # UI版本
            data = d['data']
            if d['interface'] == 'connector.entryHandler.accountRecordInfo':
                if data:
                    one = process_data(d)
                    ds.append(one)
                else:
                    print(tmp[3])

    count = 0
    key1 = {}
    key2 = {}
    for data in ds:
        mid = str(data['mid']).zfill(6)
        uid = str(data['uid']).zfill(8)
        auto_idx = str(data['auto_idx']).zfill(8)
        consume_type = data['type']
        # mid(6位，不足前面补0)-type-timestamp-auto_idx(8位，不足前面补0)
        row_key = '%(mid)s-%(consume_type)s-%(timestamp)s-%(auto_idx)s' % {'mid': mid,
                                                                           'consume_type': consume_type,
                                                                           'timestamp': data['timestamp'],
                                                                           'auto_idx': auto_idx}
        # uid(8位，不足前面补0)-timestamp-auto_idx(8位，不足前面补0)
        row_key2 = '%(uid)s-%(timestamp)s-%(auto_idx)s' % {'uid': uid, 'timestamp': data['timestamp'], 'auto_idx': auto_idx}

        # if row_key in key1:
        #     print(key1[row_key])
        #     print(data)
        #     print(row_key)
        # else:
        #     key1[row_key] = data

        if row_key2 in key2:
            print(key2[row_key2])
            print(data)
            print(row_key2)
        else:
            key2[row_key2] = data

    print(len(ds))
    print(len(key1))
    print(len(key2))


