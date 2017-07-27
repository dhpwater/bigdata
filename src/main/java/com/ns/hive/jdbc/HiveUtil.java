package com.ns.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveUtil {

	private static String  DRIVER_NAME  = "org.apache.hive.jdbc.HiveDriver";
	
	/*
	 * 加载hive库的url
	 * 默认是tat库，方便打数据 ,测试
	 */
	private static String URL = "jdbc:hive2://bsa181:10000/internal_app_bsatat" ;
	
	private static Connection con = null ;
	
	public static Connection getCon(){
		if (con == null){
			try {
				Class.forName(DRIVER_NAME);
				try {
					con = DriverManager.getConnection(URL, "", "");
					return con;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			
		}
		return con;
	}
	
	public static void destroyCon(){
		if(con != null){
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
