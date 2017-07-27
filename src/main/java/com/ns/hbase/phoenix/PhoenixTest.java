package com.ns.hbase.phoenix;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;


public class PhoenixTest {

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		
//		insert();

//		batchInsert();

		
		String tb_name = args[0];
		
		int count = Integer.valueOf(args[1]);
		
		int TOTAL_THREADS = Integer.valueOf(args[2]);
		
//		String tb_name = "TEST.TB_TEST_UPD_006" ;
//		
//		int count = 1000 ;
		
//		int TOTAL_THREADS = 100;
		ExecutorService service = Executors.newFixedThreadPool(TOTAL_THREADS);
		for (int i = 0; i < TOTAL_THREADS; i++) {
			Runnable run = new SendDataPhoenixTask(i * count, (i + 1) * count, tb_name);
			// 在未来某个时间执行给定的命令
			service.execute(run);
		}
		service.shutdown();
	}
	
	
	public static void count() throws SQLException{
		try {
			// 下面的驱动为Phoenix老版本使用2.11使用，对应hbase0.94+
			// Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");

			// phoenix4.3用下面的驱动对应hbase0.98+
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 这里配置zookeeper的地址，可单个，也可多个。可以是域名或者ip
		String url = "jdbc:phoenix:bsa180";
		// String url =
		// "jdbc:phoenix:41.byzoro.com,42.byzoro.com,43.byzoro.com:2181";
		Connection conn = DriverManager.getConnection(url);
		Statement statement = conn.createStatement();
		String sql = "select count(1) as num from test.person";
		long time = System.currentTimeMillis();
		ResultSet rs = statement.executeQuery(sql);
		while (rs.next()) {
			int count = rs.getInt("num");
			System.out.println("row count is " + count);
		}
		long timeUsed = System.currentTimeMillis() - time;
		System.out.println("time " + timeUsed + "mm");
		// 关闭连接
		rs.close();
		statement.close();
		conn.close();
	}
	
	public static void query(String sql ) throws SQLException{
		try {
			// 下面的驱动为Phoenix老版本使用2.11使用，对应hbase0.94+
			// Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
			// phoenix4.3用下面的驱动对应hbase0.98+
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 这里配置zk的地址，可单个，也可多个。可以是域名或者ip
		String url = "jdbc:phoenix:bsa180";
		Connection conn = DriverManager.getConnection(url);
		Statement statement = conn.createStatement();
		
		long time = System.currentTimeMillis();
		ResultSet rs = statement.executeQuery(sql);
		while (rs.next()) {
			// 获取core字段值
			int num = rs.getInt("IDCARDNUM");
			// 获取core字段值
			String name = rs.getString("NAME");
			
			int age = rs.getInt("AGE");
		
			System.out.println(String.format("idcardnum = %s , name = %s ,age =%s ", num , name , age));
			
		}
		long timeUsed = System.currentTimeMillis() - time;
		System.out.println("time " + timeUsed + "mm");
		// 关闭连接
		rs.close();
		statement.close();
		conn.close();
	}

	public static void insert()throws SQLException{
		try {
			// 下面的驱动为Phoenix老版本使用2.11使用，对应hbase0.94+
			// Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
			// phoenix4.3用下面的驱动对应hbase0.98+
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 这里配置zk的地址，可单个，也可多个。可以是域名或者ip
		String url = "jdbc:phoenix:bsa173";
		Connection conn = DriverManager.getConnection(url);
		Statement statement = conn.createStatement();
		
//		String sql = "UPSERT into TEST.Person (IDCardNum,Name,Age ,SEX) values (107,'小王',22 ,'男')" ;
		
//		String sql = "UPSERT into TEST.TB_TEST_UPD (CURTIMESTAMP,BYTESALL,DIP,DIP1,DIP2,DIP3,DIP4,DIP5,DIP6) values"
//				+ "('1','1','1','1','1','1','1','1','1')  " ;
		
		String sql = "UPSERT into TEST.TB_TEST_UPD_001 (curtimestamp,bytesall,dip,dip1,dip2,dip3,dip4,dip5,dip6) values"
				+ "('1','1','1','1','1','1','1','1','2')  " ;
		
		statement.executeUpdate(sql);
		conn.commit();
		
		statement.close();
		conn.close();
	}
	
	public static void batchInsert() throws SQLException{
		try {
			// 下面的驱动为Phoenix老版本使用2.11使用，对应hbase0.94+
			// Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
			// phoenix4.3用下面的驱动对应hbase0.98+
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 这里配置zk的地址，可单个，也可多个。可以是域名或者ip
		String url = "jdbc:phoenix:bsa180";
		PhoenixConnection  conn = (PhoenixConnection) DriverManager.getConnection(url);
//		Statement statement = conn.createStatement();
		conn.setAutoCommit(false);
		
//		String sql = "UPSERT into TEST.TB_TEST_UPD (IDCardNum,Name,Age ,SEX) values (?,?,?,?)" ;
		
		String sql = "" ;
		PreparedStatement ps = conn.prepareStatement(sql);
		//100条提交一次
		for(int i=1; i<1000; i++){
			
			ps.setInt(1, i);   
	        ps.setString(2, RandomStringUtils.random(5, new char[]{'a','b','c','d','e','f','g','h'}));   
	        ps.setInt(3, 8);   
	        ps.setString(4, "男");   
			
		    ps.addBatch();
		    
		    // 100条记录插入一次
		    if (i % 100 == 0){
		    	ps.executeBatch();
	            System.out.println("Rows upserted: " + i);
		        conn.commit();
		     }
		}
		
		ps.close();
		conn.close();
	}
	
	
	public static void update(String sql) throws SQLException{
		try {
			// 下面的驱动为Phoenix老版本使用2.11使用，对应hbase0.94+
			// Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
			// phoenix4.3用下面的驱动对应hbase0.98+
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 这里配置zk的地址，可单个，也可多个。可以是域名或者ip
		String url = "jdbc:phoenix:bsa180";
		PhoenixConnection  conn = (PhoenixConnection) DriverManager.getConnection(url);
		
		Statement statement = conn.createStatement();
		statement.executeUpdate(sql);
		conn.commit();
		
		statement.close();
		conn.close();
	}
}
