# encoding: utf-8
"""
表数据同步服务
@author Yuriseus
@create 16-11-23 下午5:22
"""
from config.enums import Product
from db.db_source import DBSource
from kshow_song_service import KshowSongService
from minik_song_service import MinikSongService
from util.date import DateUtil
from util.excel_util import ExcelUtil


class TableSynchronousService(object):
    def __init__(self):
        self.dashboard = None
        self.minik_song_service = None
        self.kshow_song_service = None
        self.minik = None

    def open_db(self):
        self.dashboard = DBSource.dashboard()
        self.minik_song_service = MinikSongService()
        self.kshow_song_service = KshowSongService()
        self.minik = DBSource.minik()

    def close_db(self):
        self.dashboard.close()
        self.minik_song_service.db.close()
        self.kshow_song_service.db.close()
        self.minik.close()

    def synchronous_base_common(self):
        """
        从ks_data同步
        :return:
        """
        source_table_name = 'ks_data'
        last_insert_row_id = self.get_last_insert_row_id(source_table_name)
        rows = self.kshow_song_service.query(
            "select id, text, value, value2, type from %s where id >= %s" % (source_table_name, last_insert_row_id))
        if rows:
            max_row_id = rows[-1]['id']
            for row in rows:
                # 删除不保存的字段
                del row['id']
            is_success = self.batch_insert(self.dashboard, 'base_common', rows)
            if is_success:
                self.update_table_synchronous_record(max_row_id, source_table_name, product=Product.KSHOW_SH)

    def synchronous_base_singer(self):
        """
        从mk_singer、ks_singer同步
        :return:
        """
        # minik
        source_table_name1 = 'mk_singer'
        last_insert_row_id = self.get_last_insert_row_id(source_table_name1)
        rows = self.minik_song_service.query("select id, number, name, sex, country, song_count, intro from %s where id > %s limit 5000"
                                     % (source_table_name1, last_insert_row_id))
        if rows:
            max_row_id = rows[-1]['id']
            for row in rows:
                del row['id']
                if not row['intro']:
                    row['intro'] = ''
                # 添加产品
                row['product'] = Product.MINIK
            is_success = self.batch_insert(self.dashboard, 'base_singer', rows)
            if is_success:
                self.update_table_synchronous_record(max_row_id, source_table_name1, product=Product.MINIK)

        # kshow
        source_table_name2 = 'ks_singer'
        last_insert_row_id = self.get_last_insert_row_id(source_table_name2)
        rows = self.kshow_song_service.query(
            "select id, number, name, sex, country, intro, singer_type, constellation, repr_works from %s where id >= %s"
            % (source_table_name2, last_insert_row_id))
        if rows:
            max_row_id = rows[-1]['id']
            for row in rows:
                row['name'] = row['name'].replace("'", "\'")
                if not row['intro']:
                    row['intro'] = ''
                if not row['repr_works']:
                    row['repr_works'] = ''
                if not row['singer_type']:
                    row['singer_type'] = 0
                if not row['constellation']:
                    row['constellation'] = 0
                del row['id']
                # 添加产品
                row['product'] = Product.KSHOW_SH
            is_success = self.batch_insert(self.dashboard, 'base_singer', rows)
            if is_success:
                self.update_table_synchronous_record(max_row_id, source_table_name2, product=Product.KSHOW_SH)

    def synchronous_base_song(self):
        """
        从mk_song、ks_song同步
        :return:
        """
        # minik
        source_table_name = 'mk_song'
        last_insert_row_id = self.get_last_insert_row_id(source_table_name)
        rows = self.minik_song_service.query(
            "select id, number, name, album, product, language, video_format, duration, Singer1, Singer2, Singer1ID, Singer2ID from %s where id >= %s"
            % (source_table_name, last_insert_row_id))
        if rows:
            max_row_id = rows[-1]['id']
            for row in rows:
                # 字段转化
                company = row['product']
                singer_name1 = row['Singer1']
                singer_name2 = row['Singer2']
                singer_id1 = row['Singer1ID']
                singer_id2 = row['Singer2ID']
                singer_number = str(singer_id1)
                if singer_id2:
                    singer_number += ',' + str(singer_id2)

                singer_name = singer_name1
                if singer_name2:
                    singer_name += ',' + singer_name2

                row['company'] = company
                row['singer_number'] = singer_number
                row['singer_name'] = singer_name

                del row['id']
                del row['Singer1']
                del row['Singer2']
                del row['Singer1ID']
                del row['Singer2ID']
                # 添加产品
                row['product'] = Product.MINIK

            data_list = []
            for row in rows:
                data_list.append(row.values())

            # ExcelUtil.write_to_excel('/opt/1.xlsx', data_list)
            is_success = self.batch_insert(self.dashboard, 'base_song', rows)
            if is_success:
                self.update_table_synchronous_record(max_row_id, source_table_name, product=Product.MINIK)

        # kshow
        source_table_name = 'ks_song'
        last_insert_row_id = self.get_last_insert_row_id(source_table_name)
        rows = self.kshow_song_service.query(
            "select id, number, name, album, product, language, video_format, duration from %s where id >= %s "
            % (source_table_name, last_insert_row_id))
        if rows:
            song_singers = self.kshow_song_service.get_song_singer()
            max_row_id = rows[-1]['id']
            for row in rows:
                # 字段转化
                company = row['product']
                number = row['number']  # str
                singer_number = ''
                singer_name = ''
                if number in song_singers:
                    singers = song_singers[number]
                    numbers = []
                    names = []
                    for singer in singers:
                        numbers.append(str(singer['number']))
                        names.append(singer['name'])
                    singer_number = ','.join(numbers)
                    singer_name = ','.join(names)

                row['product'] = Product.KSHOW_SH
                row['company'] = company
                row['singer_number'] = singer_number
                row['singer_name'] = singer_name
                del row['id']

            is_success = self.batch_insert(self.dashboard, 'base_song', rows)
            if is_success:
                self.update_table_synchronous_record(max_row_id, source_table_name, product=Product.KSHOW_SH)

    def synchronous_base_minik_machine(self):
        """
        同步minik机器信息
        :return:
        """
        source_table_name = 't_baseinfo_machine'
        last_insert_row_id = self.get_last_insert_row_id(source_table_name)
        rows = self.minik.query(
            "select id, mid, serialnum, song_warehouse_version, exec_version, ui_version from %s where id >= %s"
            % (source_table_name, last_insert_row_id))
        if rows:
            max_row_id = rows[-1]['id']
            for row in rows:
                # 字段转化
                for name in ['serialnum', 'song_warehouse_version', 'exec_version', 'ui_version']:
                    if not row[name]:
                        row[name] = ''
                serial_num = row['serialnum']
                row['serial_num'] = serial_num
                del row['id']
                del row['serialnum']

            data_list = []
            for row in rows:
                data_list.append(row.values())

            is_success = self.batch_insert(self.dashboard, 'base_minik_machine', rows)
            if is_success:
                self.update_table_synchronous_record(max_row_id, source_table_name, product=Product.MINIK)

    def update_base_minik_machine_by_excel(self):
        """
        根据excel信息更新机器省市等
        :return:
        """
        file_path = u'J:/meda/workspace/operation/trunk/艾美《minik》/场地信息/base_minik_machine.xlsx'
        row_list = ExcelUtil.get_data_from_excel(file_path, 2)
        datas = {}
        for row in row_list:
            # 机身ID  生产日期    机身编码    硬件版本    效果器编码   出货日期    所属行业    店名  省份  市   地址
            mid = row[0]
            if not mid:
                break
            mid = int(row[0])
            for x in range(1, 11):
                if x in [1, 5]:
                    date = ''
                    try:
                        date = DateUtil.date_format(row[x], '%Y-%m-%d')
                    except Exception:
                        print('date:%s' % row[x])
                    row[x] = date
                else:
                    if not row[x]:
                        row[x] = ''

            data = {
                'produce_date': row[1],
                'sn': row[2].strip(),
                'hardware_version': row[3].strip(),
                'effect_code': row[4].strip(),
                'ship_date': row[5],
                'industry': row[6].strip(),
                'shop_name': row[7].strip(),
                'province': row[8].strip(),
                'city': row[9].strip(),
                'address': row[10].strip(),
            }
            datas[mid] = data

        for mid, machine in datas.items():
            params = []
            keys = machine.keys()
            for k in keys:
                if k != 'mid':
                    v = machine[k]
                    if k in ['produce_date', 'ship_date'] and not v:
                        v = 'NULL'
                        params.append("%s = %s" % (k, v))
                    else:
                        params.append("%s = '%s'" % (k, v))
            sql = 'update base_minik_machine set %s where mid = %s' % (', '.join(params), mid)
            print(sql)
            self.dashboard.execute(sql)

    def get_last_insert_row_id(self, table_name, product=Product.MINIK):
        """
        获取最后插入的数据的id
        :param table_name:
        :param product:
        :return:
        """
        rows = self.dashboard.query("select * from table_synchronous_record where product = %s and table_name = '%s'"
                                    % (product, table_name))
        last_row_id = -1    # 不存在
        if rows:
            last_row_id = rows[0]['last_row_id']
        return last_row_id

    def update_table_synchronous_record(self, last_row_id, table_name, product=Product.MINIK):
        """
        获取最后插入的数据的id
        :param last_row_id:
        :param table_name:
        :param product:
        :return:
        """
        exist_row_id = self.get_last_insert_row_id(table_name, product)
        if exist_row_id != -1:
            sql = "update table_synchronous_record set last_row_id=%s where table_name = '%s' and product = %s" \
                  % (last_row_id, table_name, product)
        else:
            sql = "insert table_synchronous_record (product, table_name, last_row_id) values (%s, '%s', %s)" \
                  % (product, table_name, last_row_id)
        self.dashboard.execute(sql)

    def batch_insert(self, db, table_name, column_values):
        """
        批量插入到mysql
        :param db:
        :param table_name:
        :param column_values:
        :return:
        """
        is_success = True
        if db and table_name and column_values:
            first = column_values[0]
            keys = first.keys()
            param_name = ','.join(first.keys())
            prefix = "INSERT INTO %(table_name)s (%(param_name)s) VALUES " % {'table_name': table_name,
                                                                              'param_name': param_name}
            values = []
            for column_value in column_values:
                value_fmts = []
                for name in keys:
                    value = column_value[name]
                    if isinstance(value, str) or isinstance(value, unicode):
                        if value == 'now()':
                            value_fmts.append("%(" + name + ")s")
                        else:
                            if value.find("'") != -1:  # 字段可能包含'，导致sql报错
                                value_fmts.append('"%(' + name + ')s"')
                            else:
                                value_fmts.append("'%(" + name + ")s'")
                    else:
                        value_fmts.append("%(" + name + ")s")

                param_value = ','.join(value_fmts)
                value_fmt = " (%(param_value)s) " % {'param_value': param_value}
                value = value_fmt % column_value
                values.append(value)

            if values:
                sql = prefix + ', '.join(values)
                print(sql)
                affected_row_num = db.execute(sql)
                if not affected_row_num:
                    is_success = False
        return is_success


if __name__ == '__main__':
    service = TableSynchronousService()
    service.open_db()
    # service.synchronous_base_common()
    # service.synchronous_base_singer()
    # service.synchronous_base_song()
    # service.synchronous_base_minik_machine()
    service.close_db()

