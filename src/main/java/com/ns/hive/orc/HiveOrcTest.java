package com.ns.hive.orc;

import org.apache.log4j.Logger;

import com.ns.hive.jdbc.HiveService;

public class HiveOrcTest {

	
	public static void main(String[] args){
		
		HiveService service = new HiveService();
		
		//获取flow_info表字段
		String fieldStr = service.getFieldStr();
		
		service.createORCTrancactionTable(fieldStr);
		
		//update orc 表
		service.updateORCTable();
		
	}
}
