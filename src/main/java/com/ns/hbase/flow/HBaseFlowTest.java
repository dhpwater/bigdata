package com.ns.hbase.flow;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class HBaseFlowTest {

	public static String PREFIX = "flow";

	public static void main(String[] args) {

		String tb_name = args[0];
		
		int count = Integer.valueOf(args[1]);
		
		int thread_count = Integer.valueOf(args[2]) ;

		try {
			Connection connection = HbaseUtil.getConnection();
			createTab(connection , tb_name);
			

			// long t1 = new Date().getTime() ;
			ExecutorService service = Executors.newFixedThreadPool(thread_count);
			for (int i = 0; i < thread_count; i++) {
				Runnable run = new SendFlowDataHbaseTask(i * count, (i + 1) * count, tb_name );
				// 在未来某个时间执行给定的命令
				service.execute(run);
			}
			service.shutdown();
			
//			connection.close();
			//
			// System.out.println("All tasks have finished now.");
			// System.out.println("Main Thread Now:" +
			// Thread.currentThread().getName());
			// 等待所有工作线程结束
			// System.out.println(" take time " +(int) (new Date().getTime() -
			// t1) / 1000 + " s");

			// insert(connection);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void createTab(Connection connection, String tb_name) {
		try {
			HbaseUtil.createTable(connection, tb_name);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void insert(Connection connection) {

		// String fields = "curtimestamp | bytesall | dip | dstip_int | dport |
		// dstas | dstcityname | dstcountryname | dstsubdivisionname |
		// gapinterval | input_if | output_if | packetsall | pkt_len | protocol
		// | sip | srcip_int | sport | srcas | srccityname | srccountryname |
		// srcsubdivisionname | tcpflag | dstlocationlatitude |
		// dstlocationlongitude | first_time | last_time | srclocationlatitude |
		// srclocationlongitude | srcrouterid ";

		String fields = " curtimestamp ";

		String[] array = fields.split("\\|");

		List<String> fieldList = Lists.newArrayList();
		for (String x : array) {
			fieldList.add(StringUtils.trim(x));
		}

		System.out.println(fieldList);

		// String values = "1493335255 | 24400 | 75.1.1.205 | 1258357197 | 443 |
		// 0 | | | | 9 | 124 | 146 | 400 | 14 | 6 | 1.2.3.89 | 16909145 | 75 | 0
		// | | 澳大利亚 | | 0 | 0.0 | 0.0 | 1017952351218 | 1017952360562 |
		// -25.274398 | 133.775136 | 172163443 " ;

		String values = "1493335255";

		String[] val_array = values.split("\\|");

		List<String> valueList = Lists.newArrayList();
		for (String x : val_array) {
			valueList.add(StringUtils.trim(x));
		}

		TableName tbName = TableName.valueOf("flow_test_1");
		BufferedMutator bm = null;
		List<Put> list = null;

		try {
			bm = connection.getBufferedMutator(tbName);

			Put put = null;
			String rowKey = null;
			String date = LocalDate.now().toString();

			list = new ArrayList<Put>();
			for (int i = 500000; i < 600000; i++) {
				rowKey = PREFIX + "_" + i + "_" + System.currentTimeMillis();
				put = new Put(Bytes.toBytes(rowKey));

				for (int j = 0; j < fieldList.size(); j++) {
					byte[] qualifier = Bytes.toBytes(fieldList.get(j));

					byte[] value = Bytes.toBytes(valueList.get(j));

					put.addColumn(Bytes.toBytes("cf"), qualifier, value);
				}

				put.setDurability(Durability.SKIP_WAL);
				list.add(put);
			}

			System.out.println("Generate 100000 data " + bm.getWriteBufferSize());
			Date startTime = new Date();
			bm.mutate(list);
			System.out.println(Thread.currentThread().getName() + " Put 100000 data used "
					+ (int) (new Date().getTime() - startTime.getTime()) / 1000 + " s");
			bm.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void query() {

	}

	public static void update() {

	}
}

