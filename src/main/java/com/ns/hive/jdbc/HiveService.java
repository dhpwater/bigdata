package com.ns.hive.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * @author ryan
 *
 */

public class HiveService {
	
	private Connection con ;
	
	public HiveService(){
		con = HiveUtil.getCon();
	}
	
	
	public void closeCon(){
		HiveUtil.destroyCon();
	}
	/**
	 * tableName = flow_info
	 * @param tableName
	 */
	public void query(String tableName) {
		
		try {
			Statement stmt = con.createStatement();

			String sql = "select * from "+ tableName +" limit 1";

			ResultSet res = stmt.executeQuery(sql);

			System.out.println("Running: " + sql);

			res = stmt.executeQuery(sql);

			while (res.next()) {
				System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * tableName= flow_info
	 * @param tableName
	 */
	public void queryCount(String tableName) {

		try {
			Statement stmt = con.createStatement();

			String sql = "select count(1) from"+ tableName +" where ns_date = '20170405' ";

			ResultSet res = stmt.executeQuery(sql);

			System.out.println("Running: " + sql);

			long t1 = System.currentTimeMillis();

			res = stmt.executeQuery(sql);

			long t2 = System.currentTimeMillis();

			System.out.println("query sum : " + (t2 - t1) / 1000 + "s");

			while (res.next()) {
				System.out.println("sum :" + String.valueOf(res.getInt(1)));
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * ns_date ,ns_hour 是分区字段
	 * 获取非分区字段
	 * @return
	 */
	public  String getFieldStr() {

		String ret = null;

		try {
			Statement stmt = con.createStatement();

			String sql = "describe flow_info";

			System.out.println("Running: " + sql);

			ResultSet res = stmt.executeQuery(sql);

			res = stmt.executeQuery(sql);

			StringBuilder sb = new StringBuilder();

			StringBuilder sb1 = new StringBuilder();

			while (res.next()) {
				System.out.println(res.getString(1) + "\t" + res.getString(2));
				if (!StringUtils.equals(res.getString(1), "ns_date") && !StringUtils.equals(res.getString(1), "ns_hour")
						&& !StringUtils.startsWith(res.getString(1), "#")) {
					sb.append(res.getString(1)).append("  ").append(res.getString(2)).append("  ,");
					sb1.append(res.getString(1)).append("  ").append("  ,");
				}

			}

			ret = sb.toString().substring(0, sb.toString().length() - 1);
			String s = sb1.toString().substring(0, sb1.toString().length() - 1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return ret;
	}
	
	
	/**
	 * 创建Parquet格式table
	 * 
	 * @param fieldStr
	 */
	public void createParquetTable(String fieldStr) {


		try {
			Statement stmt = con.createStatement();

			String tableName = "flow_info_parquet_test";

			String dropsql = String.format("drop table if exists %s ", tableName);

			stmt.execute(dropsql);

			String createsql = String.format(
					"create table %s ( %s ) partitioned by( ns_date int ,ns_hour int) stored as parquet", tableName,
					fieldStr);

			System.out.println(createsql);

			stmt.execute(createsql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 创建Txt格式table
	 * 
	 * @param fieldStr
	 */
	public  void createTxtTable(String fieldStr) {

		try {
			Statement stmt = con.createStatement();

			String tableName = "flow_info_text_test";

			String dropsql = String.format("drop table if exists %s ", tableName);

			stmt.execute(dropsql);

			String createsql = String.format(
					"create table %s ( %s ) partitioned by( ns_date int ,ns_hour int) stored as TEXTFILE", tableName,
					fieldStr);

			System.out.println(createsql);

			stmt.execute(createsql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	/**
	 * 创建ORC格式table
	 * 
	 * @param fieldStr
	 */
	public void createORCTable(String fieldStr) {

		try {
			Statement stmt = con.createStatement();

			String tableName = "flow_info_orc_test";

			String dropsql = String.format("drop table if exists %s ", tableName);

			stmt.execute(dropsql);

			String createsql = String.format(
					"create table %s ( %s ) partitioned by( ns_date int ,ns_hour int) stored as orc", tableName,
					fieldStr);

			System.out.println(createsql);

			stmt.execute(createsql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	/**
	 * 创建ORC事务table
	 * @param fieldStr
	 */
	public void createORCTrancactionTable(String fieldStr) {

		try {
			Statement stmt = con.createStatement();

			String tableName = "flow_info_orc_test_trancaction";

			String dropsql = String.format("drop table if exists %s ", tableName);

			stmt.execute(dropsql);

			String createsql = String.format(
					"create table %s ( %s ) partitioned by( ns_date int ,ns_hour int) stored as orc TBLPROPERTIES ('transactional'='true') ",
					tableName, fieldStr);

			System.out.println(createsql);

			stmt.execute(createsql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void loadDataParquetTable() {

		Connection con = HiveUtil.getCon();

		try {
			Statement stmt = con.createStatement();

			String sql = "insert into table flow_info_parquet_test partition (ns_date,ns_hour) select * from flow_info limit 10000 ";

			long t1 = System.currentTimeMillis();

			stmt.execute(sql);

			long t2 = System.currentTimeMillis();

			System.out.println("query sum : " + (t2 - t1) / 1000 + "s");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void loadDataORCTable() {

		Connection con = HiveUtil.getCon();

		try {

			Statement stmt = con.createStatement();

			String sql = "insert into table flow_info_orc_test partition (ns_date,ns_hour) select * from flow_info limit 10000 ";

			long t1 = System.currentTimeMillis();

			stmt.execute(sql);

			long t2 = System.currentTimeMillis();

			System.out.println("query sum : " + (t2 - t1) / 1000 + "s");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void loadDataORCTrancactionTable() {

		Connection con = HiveUtil.getCon();

		try {

			Statement stmt = con.createStatement();

			String sql = "insert into table flow_info_orc_test_trancaction_bucket_16 select "
					+ "curtimestamp    ,bytesall    ,dip    ,dstip_int    ,dport    ,dstas    ,dstcityname    ,dstcountryname    ,"
					+ "dstsubdivisionname    ,gapinterval    ,input_if    ,output_if    ,packetsall    ,pkt_len    ,protocol    ,"
					+ "sip    ,srcip_int    ,sport    ,srcas    ,srccityname    ,srccountryname    ,srcsubdivisionname    ,"
					+ "tcpflag    ,dstlocationlatitude    ,dstlocationlongitude    ,first_time    ,last_time    ,"
					+ "srclocationlatitude    ,srclocationlongitude    ,srcrouterid " + " from flow_info limit 10 ";

			long t1 = System.currentTimeMillis();

			stmt.execute(sql);

			long t2 = System.currentTimeMillis();

			System.out.println("query sum : " + (t2 - t1) / 1000 + "s");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void loadDataTextTable() {

		Connection con = HiveUtil.getCon();

		try {

			Statement stmt = con.createStatement();

			String sql = "insert into table flow_info_text_test partition (ns_date,ns_hour) select * from flow_info limit 10000 ";

			long t1 = System.currentTimeMillis();

			stmt.execute(sql);

			long t2 = System.currentTimeMillis();

			System.out.println("query sum : " + (t2 - t1) / 1000 + "s");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void updateORCTable() {

		Connection con = HiveUtil.getCon();

		try {
			Statement stmt = con.createStatement();
			String sql = "UPDATE flow_info_orc_test SET srccountryname = 'cn' WHERE last_time = 1017952354675   ";
			stmt.execute(sql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void createCarbonTable(String fieldStr) {
		Connection con = HiveUtil.getCon();

		try {
			Statement stmt = con.createStatement();

			String tableName = "flow_info_carbon_test";

			String dropsql = String.format("drop table if exists %s ", tableName);

			stmt.execute(dropsql);

			String createsql = String.format(
					"create table %s ( %s ) partitioned by( ns_date int ,ns_hour int) stored BY 'carbondata' ",
					tableName, fieldStr);

			System.out.println(createsql);

			stmt.execute(createsql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
