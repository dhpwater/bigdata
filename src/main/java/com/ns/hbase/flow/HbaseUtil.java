package com.ns.hbase.flow;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil {

	private static final Log logger = LogFactory.getLog(HbaseUtil.class);

	// private static final String TABLE_NAME = "flow_test_2";
	private static final String COLUMN_FAMILY_NAME = "cf";

	public static void main(String[] args) throws IOException {

		Connection connection = getConnection();

		String tableName = "test_1";

		String rowKey = "9";

		String familyName = "cf";

		String columnName = "dstsubdivisionname";

		String value = "nsfocus";

		// updateTable(connection, tableName, rowKey, familyName, columnName,
		// value);

		// scanResult(connection, tableName, "0", "9");

//		scanResult(connection, tableName);

		// getResult(connection, tableName, "9");

		// getResultByColumn(connection, tableName, "9", "cf",
		// "dstsubdivisionname");

		// List<String> arr=new ArrayList<String>();
		// arr.add("cf,dstsubdivisionname,nsfocus");
		// queryByFilter(connection, tableName, arr);
		
//		scanResult(connection, "TEST.PERSON");
		
		String id_col = "IDCARDNUM";
		String id_col_val = String.valueOf(10000);
        
        String name_col = "NAME";
		String name_col_val = RandomStringUtils.random(5, new char[]{'a','b','c','d','e','f','g','h'});
        
        String age_col = "AGE";
		String age_col_val = RandomStringUtils.randomNumeric(2) ;
		
		String [] colArray = {id_col ,name_col ,age_col };
		String [] valArray = {id_col_val , name_col_val ,age_col_val};
		insert(connection, "1000", "TEST.PERSON",colArray,valArray);

		
	}

	public static Connection getConnection() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "bsa143:60000");
		conf.set("hbase.zookeeper.property.clientport", "2181");
		conf.set("hbase.zookeeper.quorum", "bsa143");

		/**
		 * HTable类读写时是非线程安全的，已经标记为Deprecated
		 * 建议通过org.apache.hadoop.hbase.client.Connection来获取实例
		 */
		Connection connection = ConnectionFactory.createConnection(conf);
		return connection;
	}

	public static void createTable(Connection connection, String tb_name) throws IOException {

		Admin admin = connection.getAdmin();

		if (!admin.tableExists(TableName.valueOf(tb_name))) {
			logger.info("To create table named " + tb_name);
			TableName tableName = TableName.valueOf(tb_name);
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			HColumnDescriptor columnDesc = new HColumnDescriptor(COLUMN_FAMILY_NAME);
			tableDesc.addFamily(columnDesc);

			// 设置5个region
//			byte[][] splitKeys = new byte[][] { Bytes.toBytes("200000"), Bytes.toBytes("400000"), Bytes.toBytes("600000"),
//					Bytes.toBytes("800000") ,Bytes.toBytes("1000000") ,Bytes.toBytes("1200000") ,Bytes.toBytes("1400000"),
//					Bytes.toBytes("1600000"),Bytes.toBytes("1800000")};

			// 直接根据描述创建表
			// admin.createTable(tableDesc);

			// 设置预分区
//			admin.createTable(tableDesc, splitKeys);
			
			
			  //注意建立预分区的startKey与endKey类型要与你插入数据时的hash值对应起来，此处是short数，那个你插入时rowkey的前缀应该是short的byte数组而不是字符串的byte数组
//            admin.createTable(tableDesc,
//                    Bytes.toBytes((int)(0)), Bytes.toBytes((int)(9)), 10);//创建表-参数分别是：表描述、起始key、结束key、分区数
					
			
			String[] keys = new String[] { "10|", "20|", "30|", "40|", "50|", "60|", "70|", "80|", "90|" };
			byte[][] splitKeys = new byte[keys.length][];
			TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);// 升序排序
			for (int i = 0; i < keys.length; i++) {
				rows.add(Bytes.toBytes(keys[i]));
			}
			Iterator<byte[]> rowKeyIter = rows.iterator();
			int i = 0;
			while (rowKeyIter.hasNext()) {
				byte[] tempRow = rowKeyIter.next();
				rowKeyIter.remove();
				splitKeys[i] = tempRow;
				i++;
			}
			
			 admin.createTable(tableDesc,splitKeys);
			
		} else {
			System.out.println("table is exist");
		}

	}

	/*
	 * 根据rwokey查询
	 * 
	 * @rowKey rowKey
	 * 
	 * @tableName 表名
	 */
	public static void getData(Connection connection, String tableName, String rowKey, String colFamily, String col)
			throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		if (colFamily != null) {
			get.addFamily(Bytes.toBytes(colFamily));
		}
		if (colFamily != null && col != null) {
			get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
		}
		Result result = table.get(get);
		showCell(result);
		table.close();
	}

	/**
	 * 格式化输出
	 * 
	 * @param result
	 */
	public static void showCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("RowName: " + new String(CellUtil.cloneRow(cell)) + " ");
			System.out.println("Timetamp: " + cell.getTimestamp() + " ");
			System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
			System.out.println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
		}
	}

	/**
	 * 更新某一列的值
	 *
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowkey
	 * @param familyName
	 *            列族
	 * @param columnName
	 *            列名
	 * @param value
	 *            值
	 */
	public static void updateTable(Connection connection, String tableName, String rowKey, String familyName,
			String columnName, String value) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
			table.put(put);
			System.out.println("Update Table Success!!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除rowkey
	 *
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowKey
	 */
	public static void deleteAllColumn(Connection connection, String tableName, String rowKey) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Delete delAllColumn = new Delete(Bytes.toBytes(rowKey));
			table.delete(delAllColumn);
			System.out.println("Delete AllColumn Success");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 删除指定列
	 *
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowKey
	 * @param familyName
	 *            列族
	 * @param columnName
	 *            列名
	 */
	public static void deleteColumn(Connection connection, String tableName, String rowKey, String familyName,
			String columnName) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Delete delColumn = new Delete(Bytes.toBytes(rowKey));
			delColumn.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			table.delete(delColumn);
			System.out.println("Delete Column Success!!!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 查询多个版本的数据
	 *
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowKey
	 * @param familyName
	 *            列族
	 * @param columnName
	 *            列名
	 */
	public static void getResultByVersion(Connection connection, String tableName, String rowKey, String familyName,
			String columnName) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			get.setMaxVersions(5);
			Result result = table.get(get);
			for (Cell cell : result.listCells()) {
				System.out.println("family:"
						+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
						cell.getQualifierLength()));
				System.out.println(
						"value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				System.out.println("Timestamp:" + cell.getTimestamp());
				System.out.println("--------------------------");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 查询某一列数据
	 *
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowKey
	 * @param familyName
	 *            列族
	 * @param columnName
	 *            列名
	 */
	public static void getResultByColumn(Connection connection, String tableName, String rowKey, String familyName,
			String columnName) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			Result result = table.get(get);
			for (Cell cell : result.listCells()) {
				System.out.println("family:"
						+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
						cell.getQualifierLength()));
				System.out.println(
						"value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				System.out.println("Timestamp:" + cell.getTimestamp());
				System.out.println("-------------------------");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 范围查询数据
	 *
	 * @param tableName
	 *            表名
	 * @param beginRowKey
	 *            startRowKey
	 * @param endRowKey
	 *            stopRowKey
	 */

	public static void scanResult(Connection connection, String tableName, String beginRowKey, String endRowKey) {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(beginRowKey));
		scan.setStopRow(Bytes.toBytes(endRowKey));
		scan.setMaxVersions(1);
		scan.setCaching(20);
		scan.setBatch(10);

		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			ResultScanner rs = table.getScanner(scan);
			for (Result result : rs) {
				System.out.println("query size :" + Bytes.toString(result.getRow()));
				// 以下是打印内容
				for (Cell cell : result.listCells()) {
					System.out.println("family:"
							+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
					System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(),
							cell.getQualifierOffset(), cell.getQualifierLength()));
					System.out.println("value:"
							+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
					System.out.println("Timestamp:" + cell.getTimestamp());
					System.out.println("---------------------");
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 全表扫描数据
	 *
	 * @param tableName
	 *            表名
	 */
	public static void scanResult(Connection connection, String tableName) {
		Scan scan = new Scan();
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("query size :" + Bytes.toString(r.getRow()));
				for (Cell cell : r.listCells()) {
					System.out.println("family:"
							+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
					System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(),
							cell.getQualifierOffset(), cell.getQualifierLength()));
					System.out.println("value:"
							+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
					System.out.println("Timestamp:" + cell.getTimestamp());
					System.out.println("------------------------------");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据rowkey查询数据
	 *
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowKey
	 * @return
	 */
	public static Result getResult(Connection connection, String tableName, String rowKey) {
		Result result = null;
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			result = table.get(get);
			for (Cell cell : result.listCells()) {
				System.out.println("family:"
						+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
						cell.getQualifierLength()));
				System.out.println(
						"value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				System.out.println("Timestamp:" + cell.getTimestamp());
				System.out.println("-------------------------------");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 删除表
	 *
	 * @param tableName
	 *            表名
	 */
	public static void deleteTable(Connection connection, String tableName) {
		try {
			Admin admin = connection.getAdmin();
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
			System.out.println(tableName + " is deleted!!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void queryByFilter(Connection connection, String tableName, List<String> arr) {

		try {
			Table table = connection.getTable(TableName.valueOf(tableName));

			FilterList filterList = new FilterList();
			Scan s1 = new Scan();

			for (String v : arr) { // 各个条件之间是“与”的关系
				String[] s = v.split(",");
				filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]),
						CompareOp.EQUAL, Bytes.toBytes(s[2])));
				// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
				// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
				s1.setFilter(filterList);
				ResultScanner ResultScannerFilterList = table.getScanner(s1);
				for (Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList.next()) {
					for (Cell cell : rr.listCells()) {
						System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
								cell.getFamilyLength()));
						System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(),
								cell.getQualifierOffset(), cell.getQualifierLength()));
						System.out.println("value:"
								+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
						System.out.println("Timestamp:" + cell.getTimestamp());
						System.out.println("-------------------------------");
					}
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void insert(Connection connection ,String rowKey, String tableName, String[] column1, 
			String[] value1) throws IOException{
		 /* get table. */
        TableName tn = TableName.valueOf(tableName);
        Table table = connection.getTable(tn);
        
        Put put = new Put(Bytes.toBytes(rowKey));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
        
        System.out.println("cf length : " + columnFamilies.length);
        
        for (int i = 0; i < columnFamilies.length; i++) {
            String f = columnFamilies[i].getNameAsString();
            System.out.println("cf : " + f);
            if (f.equals("0")) {
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
           
        }

        /* put data. */
        table.put(put);
        System.out.println("add data Success!");
	}

}
