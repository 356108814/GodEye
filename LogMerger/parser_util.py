# encoding: utf-8
import re
import json


class ParserUtil(object):

    @staticmethod
    def split(data, sep='|'):
        if data:
            return data.split(sep)
        return []

    @staticmethod
    def split_k_v(data, sep='&', kv_sep='='):
        """
        分割键值对
        :param data:
        :param sep: 分隔符
        :param kv_sep: 键值分割符
        :return: dict
        """
        result_dict = {}
        if data:
            array = data.split(sep)
            for _, kv in enumerate(array):
                if kv != '':
                    kv_array = kv.split(kv_sep)
                    k = kv_array[0]
                    if len(kv_array) == 2:
                        v = kv_array[1]
                        if v.isdigit():
                            v = int(v)
                        else:
                            try:
                                v = float(v)
                            except ValueError:
                                pass
                    else:
                        v = ''
                    result_dict[k] = v
        return result_dict

    @staticmethod
    def re_group(data, regex):
        """
        正则分组提取
        :param data:
        :param regex: 如："(?P<user_agent>.*)" (?P<request_time>.*)ms'
        :return: dict
        """
        reg = re.compile(regex)
        match = reg.match(data)
        if not match:
            # TODO log
            print(data)
        return match.groupdict()

    @staticmethod
    def get_dict(data):
        rtn = {}
        if data:
            try:
                rtn = json.JSONDecoder().decode(data)
            except Exception as e:
                pass
        return rtn


if __name__ == '__main__':
    s = 'aimei_rsp_time=&a=1'
    parser = ParserUtil()
    a, b, c = ['a', 'b', 'c']
    print(b)
    parser.get_dict('a')
