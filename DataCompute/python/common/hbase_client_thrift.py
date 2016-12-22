# encoding: utf-8
"""

@author Yuriseus
@create 16-10-20 下午4:53
"""
import struct
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation


# Method for encoding ints with Thrift's string encoding
def encode(n):
   return struct.pack("i", n)


# Method for decoding ints with Thrift's string encoding
def decode(s):
   return int(s) if s.isdigit() else struct.unpack('i', s)[0]


class HbaseClient(object):
    def __init__(self, host='localhost', port=9090):
        transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port), rbuf_size=20*1024*1024)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = Hbase.Client(protocol)
        transport.open()

    def get_tables(self):
        """
        获取所有表
        """
        return self.client.getTableNames()

    def create_table(self, table, *columns):
        """
        创建表
        """
        self.client.createTable(table, map(lambda column: ColumnDescriptor(column), columns))

    def put(self, table, row, columns, attributes=None):
        """
        添加记录
        @:param columns {"k:1":"11"}
        """
        self.client.mutateRow(table, row, map(lambda (k,v): Mutation(column=k, value=v), columns.items()), attributes)

    def batch(self, table, data_list, attributes=None):
        """
        批量提交记录
        @:param data_list dict_list {'row_key': xx, 'data': {'info:name': 'yuri'}}
        """
        mutations_batch = []
        for data in data_list:
            row_key = data['row_key']
            data = data['data']
            mutations = []
            for column, value in data.iteritems():
                mutations.append(Mutation(column=column, value=value))
            mutations_batch.append(BatchMutation(row_key, mutations))
        self.client.mutateRows(table, mutations_batch, attributes)

    def get(self, table, row, qualifier='0'):
        """ get one row from hbase table

        :param row: row key
        """
        rows = self.client.getRow(table, row, {})
        ret = []
        for r in rows:
            rd = {}
            for k, v in r.columns.items():
                rd[k] = v.value
            # for j, column in enumerate(self.columnFamilies):
            #     if self.columnFamiliesType[j] == str:
            #         rd.update({column: r.columns.get(column + ':' + qualifier).value})
            #     elif self.columnFamiliesType[j] == int:
            #         rd.update({column: decode(r.columns.get(column + ':' + qualifier).value)})
            ret.append(rd)
        return ret

    def scan(self, table, start_row="", columns=None, attributes=None):
        """
        获取记录
        """

        scanner = self.client.scannerOpen(table, start_row, columns, attributes)
        while True:
            r = self.client.scannerGet(scanner)
            if not r:
                break
            yield dict(map(lambda (k, v): (k, v.value),r[0].columns.items()))

if __name__ == "__main__":
    # client = HbaseClient("172.16.1.189", 9090)
    hbase_client = HbaseClient("CDH-2", 9090)
    hbase_client.client.disableTable('user_action')
    hbase_client.client.deleteTable('user_action')
    hbase_client.client.disableTable('user_action_metrics')
    hbase_client.client.deleteTable('user_action_metrics')
    #
    hbase_client.create_table('user_action', 'info')
    hbase_client.create_table('user_action_metrics', 'info')

    # client.put("user_action", "1", {"info:name": "click2", "info:value": '100'})
    # client.create_table("student", "name", "coruse")
    # print(client.get_tables())
    # client.put("student", "1", {"name:":"zhangsan", "coruse:art": "88", "coruse:math": "12"})
    # client.put("student", "2", {"name:":"lisi", "coruse:art": "90", "coruse:math": "100"})
    # client.put("student", "3", {"name:":"lisi2"})
    # for v in client.scan("student", columns=["name"]):
    #     print(v)
    # for v in client.scan("student"):
    #     print(v)