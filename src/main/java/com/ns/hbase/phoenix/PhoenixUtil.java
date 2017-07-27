package com.ns.hbase.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.phoenix.jdbc.PhoenixConnection;

public class PhoenixUtil {
	
	public static Connection getConnection(){
		try {
			// phoenix4.3用下面的驱动对应hbase0.98+
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 这里配置zookeeper的地址，可单个，也可多个。可以是域名或者ip
		String url = "jdbc:phoenix:bsa173";
		Connection conn = null ;
		try {
			 conn = (PhoenixConnection) DriverManager.getConnection(url);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}
	
	public static void insert()throws SQLException{
		Connection conn = getConnection();
		Statement statement = conn.createStatement();
		String sql = "UPSERT into TEST.TB_TEST_UPD_001 (curtimestamp,bytesall,dip,dip1,dip2,dip3,dip4,dip5,dip6) values"
				+ "('1','1','1','1','1','1','1','1','3')  " ;
		statement.executeUpdate(sql);
		conn.commit();
		
		statement.close();
		conn.close();
	}
	
	public static void main(String[] args)throws SQLException{
		
		insert();
	}
}
