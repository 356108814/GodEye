# encoding: utf-8
"""
impo
@author Yuriseus
@create 2016-8-17 15:02
"""
import json
import base64
import random
from json import JSONDecoder

from util.date import DateUtil

if __name__ == '__main__':
    t = unicode(u'告白')
    print(t)
    s = t + '111'
    a = isinstance(t, unicode)
    print('\xE5\x91\x8A\xE7\x99\xBD\xE6\xB0\x94\xE7\x90\x83')
    s = '\xE5\x91\x8A\xE7\x99\xBD\xE6\xB0\x94\xE7\x90\x83'
    # a = s.decode('utf-8')
    a = u'国外'
    d = {'i:%s' % s: 1}
    print(d)

    v = '\u8bf4\u8c0e'
    if isinstance(v, unicode):
        v = v.encode('utf-8')
    else:
        v = str(v)
    print(v)

    s = 'timestamp=16-10-20_22:37:30&page=play&remain_coin=0&session_id=20966&remain_time=0&remain_songs=0&uid=0&sequence_id=36&mid=990&current_coin_value=100&cur_package_id=-1&ui_version=2.0.0.28&exec_version=2.0.1.12&action=cf_song_listen&sub_page=system_logout&M014721'
    tmp = s.split('&')
    tmp[len(tmp) - 1] = 'song_id=%s' % tmp[len(tmp) - 1]
    line_data = '&'.join(tmp)
    print(line_data)
    a = '\x5Cu8bf4\x5Cu8c0e'
    print(a.encode('utf-8').decode('utf-8'))
    u = '\xE6\x98\x9F\xE6\x9C\x9F\xE5\x9B\x9B'
    print(a.decode('utf-8').decode('utf-8'))
    print(random.randint(1, 5))

    s = '{"current_coin_value": 100, "remain_time": "18"}'   # json标准格式只能用双引号
    o = json.loads(s)
    # o = JSONDecoder().decode(s)
    print(o)

    d = {"a": 1}
    print(str(d))

    if 0:
        print(11)

    d = '2016-10-10 22:22:22'
    print(d[11:13])

    print('\xE6\x98\x9F\xE6\x9C\x9F\xE5\x9B\x9B')

    a = float('1')
    b = 2
    print(a+b)
    print('1.90'.find('.'))
    s = '\u661f\u671f\u56db'
    s = s.decode('unicode-escape')
    print('\'')
    a = "Boy'z"
    a = a.replace("'", "`")
    print(a)