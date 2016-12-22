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
    # 增加登陆日期
    timestamp = int(d['timestamp'])
    if 'stime' in data:
        timestamp = int(d['stime'])
    d['date'] = DateUtil.timestamp2date(timestamp, '%Y-%m-%d %H:%M:%S')
    d['hour'] = d['date'][0:13]  # 用于按小时统计
    # 增加星期
    weekday_int = DateUtil.str2date(d['date'], '%Y-%m-%d %H:%M:%S').isoweekday()
    d['weekday'] = WEEKDAY[weekday_int]
    uid = data['uid']  # str
    if uid == '':
        uid = '0'
    d['uid'] = uid
    d['mid'] = str(data['mid'])
    d['songid'] = data['songid']
    if 'stime' in data:  # 后续版本才加的，历史数据没有该项
        d['start_timestamp'] = str(data['stime'])
        d['end_timestamp'] = str(int(data['stime']) + int(data['playdur']))
        d['dur_time'] = str(data['durtime'])
        d['song_time'] = str(data['songdur'])
    d['is_finish'] = data['isfinish']
    d['is_free'] = data['isfree']
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
            if d['interface'] == 'connector.playerHandler.playerFinishSong' and data:
                # print(data)
                one = process_data(d)
                ds.append(one)
                count += 1
    count = 0
    for d in ds:
        if d['mid'] == '388' and d['hour'].find('2016-10-20') != -1 and d['songid'] == 'C004482':
            print(d)
            count += 1
    print(count)


