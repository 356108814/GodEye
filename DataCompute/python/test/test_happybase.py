# encoding: utf-8
"""

@author Yuriseus
@create 16-11-1 下午5:22
"""
import happybase


if __name__ == '__main__':
    table_name = 'minik_user_action'
    connection = happybase.Connection('CDH-1')
    # connection.open()
    table = connection.table(table_name)
    # connection.delete_table(table_name, True)
    # connection.create_table(table_name, {'info': dict()})
    # row = table.row('20161012')
    # print(row)
    # print(bytearray(row['info:time']).decode('utf-8'))
    # print(int(row['info:login_session_count'], 16) )
    # print(table.counter_get('20161012', 'info:login_time'))
    # c = table.counter_get('20161020', 'info:session_count')
    # print(c)
    result = table.scan('00000000-1476795602', '99999999-1479259859')
    count = 0
    for r in result:    # 迭代器，每次查询1000条记录，由scan的batch_size决定
        print(r)
        count += 1
        print(count)
