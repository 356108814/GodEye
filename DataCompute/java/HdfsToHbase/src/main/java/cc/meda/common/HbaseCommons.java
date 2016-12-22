package cc.meda.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class HbaseCommons {
	public static Configuration config = null;

	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "CDH-0:2181");
	}

	/**
	 * 删除rowkey
	 *
	 * @param tableName 表名
	 * @param rowKey    rowKey
	 */
	public static void deleteAllColumn(String tableName, String rowKey) {
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			Delete delAllColumn = new Delete(Bytes.toBytes(rowKey));
			table.delete(delAllColumn);
			System.out.println("Delete AllColumn Success");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != table) {
					table.close();
				}
				if (null != connection && !connection.isClosed()) {
					System.out.println("deleteAllColumn is closed");
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 删除指定列
	 *
	 * @param tableName  表名
	 * @param rowKey     rowKey
	 * @param familyName 列族
	 * @param columnName 列名
	 */
	public static void deleteColumn(String tableName, String rowKey, String familyName, String columnName) {
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			Delete delColumn = new Delete(Bytes.toBytes(rowKey));
			delColumn.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			table.delete(delColumn);
			System.out.println("Delete Column Success!!!");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != table) {
					table.close();
				}
				if (null != connection && !connection.isClosed()) {
					System.out.println("deleteColumn is closed");
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 查询多个版本的数据
	 *
	 * @param tableName  表名
	 * @param rowKey     rowKey
	 * @param familyName 列族
	 * @param columnName 列名
	 */
	public static void getResultByVersion(String tableName, String rowKey, String familyName, String columnName) {
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			get.setMaxVersions(5);
			Result result = table.get(get);
			for (Cell cell : result.listCells()) {
				System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				System.out.println("Timestamp:" + cell.getTimestamp());
				System.out.println("------------------------");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != table) {
					table.close();
				}
				if (null != connection && !connection.isClosed()) {
					System.out.println("getResultByVersion is closed");
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * 更新某一列的值
	 *
	 * @param tableName  表名
	 * @param rowKey     rowkey
	 * @param familyName 列族
	 * @param columnName 列名
	 * @param value      值
	 */
	public static void updateTable(String tableName, String rowKey, String familyName, String columnName, String value) {
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
			table.put(put);
			System.out.println("Update Table Success!!");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != table) {
					table.close();
				}
				if (null != connection && !connection.isClosed()) {
					System.out.println("updateTable is  closed");
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * 查询某一列数据
	 *
	 * @param tableName  表名
	 * @param rowKey     rowKey
	 * @param familyName 列族
	 * @param columnName 列名
	 */
	public static void getResultByColumn(String tableName, String rowKey, String familyName, String columnName) {
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			Result result = table.get(get);
			for (Cell cell : result.listCells()) {
				System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				System.out.println("Timestamp:" + cell.getTimestamp());
				System.out.println("-----------------------");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != table) {
					table.close();
				}
				if (null != connection && !connection.isClosed()) {
					System.out.println("getResultByColumn is close");
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 范围查询数据
	 *
	 * @param tableName   表名
	 * @param beginRowKey startRowKey
	 * @param endRowKey   stopRowKey
	 */
	public static void scanResult(String tableName, String beginRowKey, String endRowKey) {
		Connection connection = null;
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(beginRowKey));
		scan.setStopRow(Bytes.toBytes(endRowKey));
		ResultScanner rs = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			rs = table.getScanner(scan);
			for (Result result : rs) {
				for (Cell cell : result.listCells()) {
					System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
					System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
					System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
					System.out.println("Timestamp:" + cell.getTimestamp());
					System.out.println("----------------------");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != rs) {
				rs.close();
			}
			try {
				if (null != table) {
					table.close();
				}
				if (null != connection && !connection.isClosed()) {
					System.out.println("scanResult is close");
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 全表扫描数据
	 *
	 * @param tableName 表名
	 */
	public static void scanResult(String tableName) {
		Connection connection = null;
		Scan scan = new Scan();
		ResultScanner rs = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			rs = table.getScanner(scan);
			for (Result r : rs) {
				for (Cell cell : r.listCells()) {
					System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
					System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
					System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
					System.out.println("Timestamp:" + cell.getTimestamp());
					System.out.println("-----------------------------");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != rs) {
				rs.close();
			}
			try {
				if (null != table) {
					table.close();
				}
				if (null != connection && !connection.isClosed()) {
					System.out.println("scan Result is closed");
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 根据rowkey查询数据
	 *
	 * @param tableName 表名
	 * @param rowKey    rowKey
	 * @return
	 */
	public static Result getResult(String tableName, String rowKey) {
		Connection connection = null;
		Result result = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			Get get = new Get(Bytes.toBytes(rowKey));
			table = connection.getTable(TableName.valueOf(tableName));
			result = table.get(get);
			for (Cell cell : result.listCells()) {
				System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				System.out.println("Timestamp:" + cell.getTimestamp());
				System.out.println("----------------------------");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != table) {
					table.close();
				}
				if (null != connection && !connection.isClosed()) {
					System.out.println("getResult is  closed");
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	/**
	 * 添加数据
	 *
	 * @param rowKey    rowKey
	 * @param tableName 表名
	 * @param column    列名
	 * @param value     值
	 */
	public static void addData(String rowKey, String tableName, String[] column, String[] value) {
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			Put put = new Put(Bytes.toBytes(rowKey));
			table = connection.getTable(TableName.valueOf(tableName));
			HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
			for (int i = 0; i < columnFamilies.length; i++) {
				String familyName = columnFamilies[i].getNameAsString();
				if (familyName.equals("info")) {
					for (int j = 0; j < column.length; j++) {
						put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
					}
				}
				table.put(put);
				System.out.println("Add Data Success!!!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != table) {
					table.close();
				}
				if (null != connection && !connection.isClosed()) {
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

    /**
     * 批量添加数据
     * @param tableName
     * @param dataList 格式： [{"row_key": "row1", "data": {"info:name": "yuri"}}]
     */
    public static void batchAddData(String tableName, List<Map<String, Object>> dataList) {
        Long startTime = System.currentTimeMillis();
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> rowList = new ArrayList<Put>();
            int size = dataList.size();
            for(Map<String, Object> rowMap: dataList){
                String rowKey = (String) rowMap.get("rowKey");
                Map<String, String> rowData = (Map<String, String>) rowMap.get("data");
                for (Map.Entry<String, String> row : rowData.entrySet()) {
                    String[] tmp = row.getKey().split(":");    // 格式：info:name
                    String cf = tmp[0];
                    String qualifier = tmp[1];
                    String value = row.getValue();
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                    rowList.add(put);
                }
            }
            table.put(rowList);
            System.out.println(size + " line cost time: " + (System.currentTimeMillis() - startTime)/1000);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != table) {
                    table.close();
                }
                if (null != connection && !connection.isClosed()) {
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 批量添加数据
     * @param tableName
     * @param size
     */
    public static void batchAddDataTest(String tableName, int size) {
        Long startTime = System.currentTimeMillis();
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> rowList = new ArrayList<Put>();
            for (int i= 0; i < size; i++){
                Put put = new Put(Bytes.toBytes("row" + i));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name" + i), Bytes.toBytes("value" + i));
                rowList.add(put);
            }
            System.out.println(size + " line cost time: " + (System.currentTimeMillis() - startTime)/1000);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != table) {
                    table.close();
                }
                if (null != connection && !connection.isClosed()) {
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

	/**
	 * 删除表
	 *
	 * @param tableName 表名
	 */
	public static void deleteTable(String tableName) {
		Connection connection = null;
		Admin admin;
		try {
			connection = ConnectionFactory.createConnection(config);
			admin = connection.getAdmin();
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
			System.out.println(tableName + " is deleted!!");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

			try {
				if (null != connection && !connection.isClosed()) {
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 创建Table
	 *
	 * @param tableName 表名
	 * @param family    列族
	 */
	public static void createTable(String tableName, String[] family) {
		Admin admin;
		HTableDescriptor table;
		TableName t_name = TableName.valueOf(tableName);
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			admin = connection.getAdmin();
			table = new HTableDescriptor(t_name);
			for (int i = 0; i < family.length; i++) {
				table.addFamily(new HColumnDescriptor(family[i]));
			}
			if (admin.tableExists(t_name)) {
				System.out.println("Table Exists!!");
				System.exit(0);
			} else {
				admin.createTable(table);
				System.out.println("Create Table Success!!! Table Name :[ " + tableName + " ]");
			}
			admin.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != connection && !connection.isClosed()) {
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	public static void main(String[] args) throws IOException {
        String[] column = {"action_type", "action_name", "action_value", "date"};
        String[] value = {"click", "home", "", "2016-10-10 18:11:00"};
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 2; i++){
            value[2] = i + "";
            addData("10000_147644422"+i, "test_user_action", column, value);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime)/1000);
        /*
        String[] family = {"userinfo"};
        createTable("user1", family);
        */
 
        /*
        String[] column = {"name", "age", "email", "phone"};
        String[] value = {"zhangsan", "22", "zs@163.com", "13111009988"};
        String[] value1 = {"lisi", "25", "ls@163.com", "13311009988"};
        String[] value2 = {"wangwu", "27", "ww@163.com", "13811009988"};
        addData("rowkey1", "user1", column, value);
        addData("rowkey2", "user1", column, value1);
        addData("rowkey3", "user1", column, value2);
        */
 
        /*
        getResult("user1", "rowkey2");
        */
 
        /*
        scanResult("user1");
        */
 
        /*
        scanResult("user1", "rowkey1", "rowkey3");
        */
 
        /*
        getResultByColumn("user1", "rowkey1", "userinfo", "name");
        updateTable("user1", "rowkey1", "userinfo", "name", "zs");
        getResultByColumn("user1", "rowkey1", "userinfo", "name");
        */
 
        /*
        getResultByVersion("user1", "rowkey1", "userinfo", "name");
        */
 
        /*
        deleteColumn("user1","rowkey1","userinfo","email");
        */
 
        /*
        deleteAllColumn("user1","rowkey1");
        */

	}
}