package com.ns.hbase.jdbc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseJDBCTest {

	public static Logger logger = Logger.getLogger(HBaseJDBCTest.class);

	private static final String TABLE_NAME = "m_domain";

	private static final String COLUMN_FAMILY_NAME = "cf";

	public static void main(String[] args) {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "bsa180:60000");
		conf.set("hbase.zookeeper.property.clientport", "2181");
		conf.set("hbase.zookeeper.quorum", "bsa180");

		HBaseServie hbaseServie = new HBaseServie();

		Connection connection;
		try {
			
			connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();
			
			/**
             *  列出所有的表
             */
			hbaseServie.listTables(admin);
			
			  /**
             * 判断表m_domain是否存在
             */
            boolean exists = hbaseServie.isExists(admin);
            
            
            /**
             * 存在就删除
             */
            if (exists) {
            	hbaseServie.deleteTable(admin);
            } 
            
            /**
             * 创建表
             */
            hbaseServie.createTable(admin);
             
            /**
             *  再次列出所有的表
             */
            hbaseServie.listTables(admin);
            
            /**
             * 添加数据
             */
            hbaseServie.putDatas(connection);
            
            /**
             * 检索数据-表扫描
             */
            hbaseServie.scanTable(connection);
            
            /**
             * 检索数据-单行读
             */
            hbaseServie.getData(connection);
             
            /**
             * 检索数据-根据条件
             */
//            hbaseServie.queryByFilter(connection);
            
            /**
             * 删除数据
             */
            hbaseServie.deleteDatas(connection);
            
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
