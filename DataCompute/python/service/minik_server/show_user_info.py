# encoding: utf-8
"""

@author Yuriseus
@create 16-11-17 下午6:30
"""
import json

from config.enums import WEEKDAY
from util.date import DateUtil


class ShowUserInfoService(object):
    def __init__(self):
        pass

    def split(self, line):
        d = {}
        tmp = line.split('&')
        d['timestamp'] = tmp[0]
        d['mid'] = tmp[1]
        d['interface'] = tmp[2]
        try:
            # 因为有异常情况导致解析的日志中的格式是错误的
            d['data'] = json.loads(tmp[3])    # 字典
        except Exception:
            d['data'] = None
        d['exec_version'] = tmp[4]    # 程序版本
        d['ui_version'] = tmp[5]    # UI版本
        return d

    def process_data(self, d):
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
        d['date'] = data['player']['current_login_time']
        d['timestamp'] = DateUtil.date2timestamp(d['date'], '%Y-%m-%d %H:%M:%S')
        d['day'] = d['date'][0:10]    # 用于统计
        # 增加星期
        weekday_int = DateUtil.str2date(d['date'], '%Y-%m-%d %H:%M:%S').isoweekday()
        d['weekday'] = WEEKDAY[weekday_int]
        uid = data['player']['uid']    # str
        d['uid'] = uid
        return d

    def filter(self, d):
        """
        过滤，返回True的是需要处理的
        :param d:
        :return:
        """
        data = d['data']
        if d['interface'] == 'onShowUserInfo' and data:
            return True
        return False
