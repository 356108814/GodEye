# coding:utf-8
from Queue import Queue
import pymysql


class DBMySQL(object):
    """
    数据库操作基类
    """
    def __init__(self, host, port, user, password, db, pool_size=5):
        self._pool = Queue(maxsize=pool_size)
        self._host = host
        self._port = int(port)
        self._user = user
        self._password = password
        self._db = db

        self._connection = None
        self.init_pool(pool_size)

    def init_pool(self, pool_size):
        for x in range(pool_size):
            self.put_to_pool(self.create_connection())

    def put_to_pool(self, conn):
        try:
            self._pool.put_nowait(conn)
        except Exception as e:
            raise Exception('put_to_pool error')

    def create_connection(self):
        connection = pymysql.connect(host=self._host,
                                     port=self._port,
                                     user=self._user,
                                     password=self._password,
                                     db=self._db,
                                     autocommit=True,
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)
        return connection

    @property
    def connection(self):
        self._connection = self._pool.get(True)
        if not self.ping():
            self._connection.connect()
        return self._connection

    def ping(self):
        try:
            return self._connection.query('SELECT 1')
        except Exception:
            return None

    def query(self, sql, params_dict=None, is_fetchone=False):
        """
        执行sql查询语句。如：select * from user where name = '{name}'
        @param sql sql语句
        @param params_dict 参数字典
        @param is_fetchone 是否只返回一条记录
        @return dict列表或dict
        """
        result = None
        connection = self.connection
        with connection.cursor() as cursor:
            try:
                cursor.execute(sql, params_dict)
                if is_fetchone:
                    result = cursor.fetchone()
                else:
                    result = cursor.fetchall()
            except Exception as e:
                raise e
        self.put_to_pool(connection)
        return result

    def execute(self, sql, params_dict=None):
        """
        执行sql语句
        @param sql:
        @param params_dict:
        @return:
        """
        is_success = False
        connection = self.connection
        with connection.cursor() as cursor:
            try:
                # sql 格式INSERT applicant (from_type, name) VALUES (%(from_type)s, %(name)s)
                # params_dict 格式 {'from_type': 1, 'name': 'yuri'}
                cursor.execute(sql, params_dict)
                is_success = True
                connection.commit()
            except Exception as e:
                raise e
        self.put_to_pool(connection)
        return is_success

    def close(self):
        while not self._pool.empty():
            self._pool.get(True).close()


if __name__ == '__main__':
    pass
    # for x in range(100):
    #     dbMySQL = DBMySQL(host='localhost', port=3306, user='root', password='root', db='test')
    #     sql = "insert applicant(name) values ('%s')" % x
    #     r = dbMySQL.execute(sql)
    #     print(r)

