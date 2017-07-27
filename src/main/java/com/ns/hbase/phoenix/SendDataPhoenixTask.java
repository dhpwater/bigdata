package com.ns.hbase.phoenix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import org.apache.commons.lang.RandomStringUtils;

public class SendDataPhoenixTask implements Runnable {
	
	private int start ;
	
	private int count ;
	
	private Connection connection ;
	
	private String tb_name ;
	

	public SendDataPhoenixTask(int start , int count , String tb_name ){
		this.start = start ;
		this.count = count ;
		this.connection = PhoenixUtil.getConnection();
		this.tb_name = tb_name ;
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			connection.setAutoCommit(false);
			String sql = "UPSERT into "+ tb_name +" (curtimestamp,bytesall,dip,dip1,dip2,dip3,dip4,dip5,dip6) values"
					+ "(?,?,?,?,?,?,?,?,?)  " ;
			PreparedStatement ps = connection.prepareStatement(sql);
			Date startTime = new Date();
			for (int i = start; i <  count; i++) {
				ps.setString(1, String.valueOf(i));   
		        ps.setString(2,  String.valueOf(i));   
		        ps.setString(3,  String.valueOf(i));   
		        ps.setString(4, String.valueOf(i)); 
		        ps.setString(5, String.valueOf(i));   
		        ps.setString(6,  String.valueOf(i));   
		        ps.setString(7,  String.valueOf(i));   
		        ps.setString(8, String.valueOf(i)); 
		        ps.setString(9, RandomStringUtils.randomNumeric(1)); 
		        ps.addBatch();
		        // 1000 条记录插入一次
			    if (i % 1000 == 0){
			    	ps.executeBatch();
		            connection.commit();
			     }
			}
			ps.executeBatch();
			connection.commit();
			System.out.println(Thread.currentThread().getName() + " Put " + count + " data used "
					+ (int) (new Date().getTime() - startTime.getTime()) / 1000 + " s");
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	
}
