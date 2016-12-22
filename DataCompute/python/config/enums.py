# encoding: utf-8
"""
公共枚举
@author Yuriseus
@create 2016-8-18 22:57
"""


class Product(object):
    KSHOW_SH = 1
    MINIK = 2
    KSHOW_GZ = 3    # KSHOW之王
    LEFANG = 4


class Terminal(object):
    MACHINE = 1    # 实体机
    WEIXIN = 2
    APP = 3


# 消费类型
class Consume(object):
    SEARCH = 0    # 查询
    ADVANCE = 1    # 预划扣   用户一次投币或者预划扣的钱
    COST = 2    # 消费    用户消费
    CHARGE_AGAINST = 3    # 冲销    用户未消费完的冲销行为
    RECHARGE = 4    # 充值    用户充值行为


class Money(object):
    INSERT_COIN = 0    # 投币
    K_GOLD = 1    # k金币
    SILVER = 2    # 银币
    SCORE = 3    # 音乐积分
    WXPAY = 4    # 微信支付
    ALIPAY = 5       # 支付宝支付
    FREE_TICKET = 6       # 免费券


class Action(object):
    REQUEST_SONG = 0    # 点播歌曲
    LOGIN = 1
    LOGOUT = 2
    CONSUME = 3
    CLICK_BUTTON = 4
    VIEW_SONG = 5       # 查看歌曲


class UserType(object):
    ANONYMOUS = 0
    LOGIN = 1


class UserCountType(object):
    """
    用户数量类型
    """
    ADD = 1    # 新增
    LOST = 2   # 流失
    TOTAL = 3  # 总计


class UserActiveType(object):
    """
    用户活跃类型
    """
    DAU = 1    # 日活
    WAU = 2   # 周活
    MAU = 3  # 月活
    SAU = 4  # 季活


WEEKDAY = {
    1: u'星期一',
    2: u'星期二',
    3: u'星期三',
    4: u'星期四',
    5: u'星期五',
    6: u'星期六',
    7: u'星期日'
}
