# encoding: utf-8

import calendar
import datetime
import time


class DateUtil(object):

    @staticmethod
    def get_interval_day_date(start_time, days=-7, fmt='%Y-%m-%d'):
        """
        获取间隔intervalDay天后的日期
        @param start_time 起始日期
        @param days 间隔天数
        """
        # TODO 优化
        end_time = start_time + datetime.timedelta(days=days)   # datetime类型
        timestamp = int(time.mktime(end_time.timetuple()))
        time_array = time.localtime(timestamp)
        return time.strftime(fmt, time_array)

    @staticmethod
    def date_format(date_time, format_str='%Y-%m-%d'):
        """
        日期格式化
        """
        timestamp = int(time.mktime(date_time.timetuple()))
        time_array = time.localtime(timestamp)
        return time.strftime(format_str, time_array)

    @staticmethod
    def utc2local(utc_timestamp):
        """
        UTC时间戳转本地时间
        @param utc_timestamp utc时间戳。毫秒
        """
        time_tuple = time.localtime(utc_timestamp/1000)
        return time.strftime('%Y-%m-%d %H:%M:%S', time_tuple)

    @staticmethod
    def utc_str_to_local(utc_str):
        """
        UTC时间字符串转本地时间（+8:00）
        @param utc_str 格式如：2016-02-03T09:00:00.000Z
        """
        UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
        # LOCAL_FORMAT = '%Y-%m-%d %H:%M:%S'
        utc_strptime = datetime.datetime.strptime(utc_str, UTC_FORMAT)
        now_stamp = time.time()
        local_time = datetime.datetime.fromtimestamp(now_stamp)
        utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
        # 获取时区偏移量
        offset = local_time - utc_time
        local_strptime = utc_strptime + offset
        # 格式化
        return local_strptime.strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def is_valid_date(datestr):
        """
        判断是否是一个有效的日期字符串
        @param datestr 日期字符串
        """
        try:
            time.strptime(datestr, "%Y-%m-%d")
            return True
        except:
            return False

    @staticmethod
    def get_range_date_list(start_date, end_date, fmt='%Y-%m-%d'):
        """
        获取日期范围内所有日期
        @param start_date 格式：2016-01-19
        @param end_date 格式：2016-01-19
        @param fmt 日期格式
        @return list
        """
        date_list = []
        start_date = datetime.datetime.strptime(start_date, fmt).date()
        end_date = datetime.datetime.strptime(end_date, fmt).date()
        for year in range(start_date.year, end_date.year+1):
            for month in range(1, 13):

                if year == start_date.year:
                    is_need_add = month >= start_date.month
                else:
                    is_need_add = month <= end_date.month
                if is_need_add:
                    raw_date_list = DateUtil.get_date_list(year, month)
                    for _, rdate in enumerate(raw_date_list):
                        if start_date <= rdate <= end_date:
                            date_list.append(rdate.strftime(fmt))
        return date_list

    @staticmethod
    def get_day_list(year, month):
        """
        指定月内所有天列表
        @param year
        @param month
        """
        return range(calendar.monthrange(year, month)[1]+1)[1:]

    @staticmethod
    def get_date_list(year, month):
        """
        获取指定月所有的日期列表
        @param year
        @param month
        @return list 元素为datetime.date
        """
        date_list = []
        for raw_date in calendar.Calendar().itermonthdates(year, month):
            r_year = raw_date.year
            r_month = raw_date.month
            if r_year == year and r_month == month:
                date_list.append(raw_date)
        return date_list

    @staticmethod
    def date2timestamp(date_str, fmt='%Y-%m-%d'):
        time_array = time.strptime(date_str, fmt)
        timestamp = int(time.mktime(time_array))
        return timestamp

    @staticmethod
    def timestamp2date(timestamp, fmt='%Y-%m-%d'):
        time_array = time.localtime(timestamp)
        return time.strftime(fmt, time_array)

    @staticmethod
    def now(fmt='%Y-%m-%d'):
        return datetime.datetime.now().strftime(fmt)

    @staticmethod
    def now_timestamp():
        # return int(time.mktime(datetime.datetime.now().timetuple()))
        return int(time.time())


dateUtil = DateUtil()

if __name__ == '__main__':
    utc_time = 1454490000000
    #1454490000000
    #2016-02-03T09:00:00.000Z
    utc_time_str = '2016-02-03T09:00:00.000Z'
    # UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
    # LOCAL_FORMAT = '%Y-%m-%d %H:%M:%S'
    # utc_st = datetime.datetime.strptime(utc_time_str, UTC_FORMAT)
    
    # local_time = dateUtil.utc2local(utc_st)
    # print local_time.strftime('%Y-%m-%d %H:%M:%S')

    # print dateUtil.utc2local(utc_time_str)

    timeTuple = time.localtime(utc_time/1000)
    print(time.strftime('%Y-%m-%d %H:%M:%S', timeTuple))
    # print dateUtil.is_valid_date('2016-02-29')
    print(dateUtil.get_range_date_list('2015-02-24', '2016-03-02'))
    print(dateUtil.timestamp2date(1472601600))
