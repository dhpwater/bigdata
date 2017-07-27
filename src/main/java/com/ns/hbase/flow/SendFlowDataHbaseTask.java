package com.ns.hbase.flow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class SendFlowDataHbaseTask implements Runnable {

	private int start ;
	
	private int count ;
	
	private String tb_name ;
	
	private Connection connection ;
	
	public SendFlowDataHbaseTask(int start , int count , String tb_name ){
		this.start = start ;
		this.count = count ;
		try {
			this.connection = HbaseUtil.getConnection();
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.tb_name = tb_name ;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
//		String fields = "curtimestamp  | bytesall  |     dip     |  dstip_int  | dport  | dstas  | dstcityname  | dstcountryname  | dstsubdivisionname  | gapinterval  | input_if  | output_if  | packetsall  | pkt_len  | protocol  |    sip    | srcip_int  | sport  | srcas  | srccityname  | srccountryname  | srcsubdivisionname  | tcpflag  | dstlocationlatitude  | dstlocationlongitude  |   first_time   |   last_time    | srclocationlatitude  | srclocationlongitude  | srcrouterid  ";
		
		
//		String fields = " curtimestamp ";
		
		String fields = "curtimestamp  | bytesall  |     dip     |  dstip_int  | dport  | dstas  | dstcityname  | dstcountryname  | dstsubdivisionname" ;
		
		String[] array = fields.split("\\|");
		
		List<String> fieldList = Lists.newArrayList();
		for(String x : array){
			fieldList.add(StringUtils.trim(x));
		}
		
		
//		String values = "1493335255    | 24400     | 75.1.1.205  | 1258357197  | 443    | 0      |              |                 |                     | 9            | 124       | 146        | 400         | 14       | 6         | 1.2.3.89  | 16909145   | 75     | 0      |              | 澳大利亚            |                     | 0        | 0.0                  | 0.0                   | 1017952351218  | 1017952360562  | -25.274398           | 133.775136            | 172163443  " ;
		
//		String values = "1493335255" ;
		
		String values = "1493335255    | 24400     | 75.1.1.205  | 1258357197  | 443    | 0      |              |                 |                     " ;
		
		String[] val_array = values.split("\\|");
		
		List<String> valueList = Lists.newArrayList();
		for(String x : val_array){
			valueList.add(StringUtils.trim(x));
		}
		
		
		TableName tbName = TableName.valueOf(tb_name);
//		 table.setWriteBufferSize(writeBuffer);
		BufferedMutator bm = null;
		List<Put> list = null;
		
		try {
			bm = connection.getBufferedMutator(tbName);
			
			Put put = null;
			String rowKey = null;

			list = new ArrayList<Put>();
			for (int i = start; i <  count; i++) {
				rowKey =  String.valueOf(i)  ;
				
//				put = new Put(Bytes.toBytes(rowKey));
				
				put = new Put(Bytes.toBytes(RandomStringUtils.randomNumeric(1) +  i));
				
	            for(int j =0 ; j < fieldList.size() ; j ++){
	            	 byte [] qualifier = Bytes.toBytes(fieldList.get(j));
	            	 
	            	 byte [] value = Bytes.toBytes(valueList.get(j));
	            	 
	            	 put.addColumn(Bytes.toBytes("cf"), qualifier, value);
	            }
	            
				put.setDurability(Durability.SKIP_WAL);
				list.add(put);
			}

			System.out.println("Generate " + count + " data " + bm.getWriteBufferSize());
			Date startTime = new Date();
			bm.mutate(list);
			System.out.println(Thread.currentThread().getName() + " Put " + count + " data used "
					+ (int) (new Date().getTime() - startTime.getTime()) / 1000 + " s");
			bm.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

}
