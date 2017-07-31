package com.ns.spark;


import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.ns.hive.Person;

import scala.Tuple2;

/**
 * 
 * 从kafka 接收数据，写入hive
 * @author ryan
 *
 */

public class KafkaToHive {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("KafkaToHiveTest").set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");

		SparkContext sc = new SparkContext(sparkConf);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		final HiveContext hc = new HiveContext(sc);
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(10000));

		// 接收数据的地址和端口
		String zkQuorum = "10.48.183.169:2181";

		// 话题所在的组
		String group = "bsa_test";

		String topics = "person_top";

		// 每个话题的分片数
		int numThreads = 2;

		// 存放话题跟分片的映射关系
		Map<String, Integer> topicmap = new HashMap<>();
		topicmap.put(topics, numThreads);

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group,
				topicmap);

		JavaDStream<Person> lines = messages.map(new Function<Tuple2<String, String>, Person>() {
			public Person call(Tuple2<String, String> tuple2) {
				String [] str = tuple2._2().split(",");
				Person p = new Person() ;
				p.setId(Integer.parseInt(str[0]));
				p.setName(str[1]);
				p.setAge(Integer.parseInt(str[2]));
				return p ;
			}
		});

		lines.foreachRDD(new VoidFunction<JavaRDD<Person>>() {

			@Override
			public void call(JavaRDD<Person> rdd) throws Exception {
				// TODO Auto-generated method stub

				DataFrame df = hc.createDataFrame(rdd, Person.class);

				String tempTableName = "person_tmp";

				String sql = "insert into table yz_test.parquet_person select id , name , age from person_tmp";

				df.registerTempTable(tempTableName);

				hc.sql(sql);
			}
		});
		
		jssc.start();
		
		jssc.awaitTermination();

	}

}

