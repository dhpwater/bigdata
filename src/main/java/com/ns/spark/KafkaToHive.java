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

import com.alibaba.fastjson.JSON;
import com.ns.hive.Person;

import scala.Tuple2;

/**
 * KafkaUtils.createStream 将数据从kafka 写入hive
 * 
 * @author ryan
 *
 */
public class KafkaToHive {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		
		SparkContext sc = new SparkContext(sparkConf);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		final HiveContext hc = new HiveContext(sc);
		hc.setConf("parquet.memory.min.chunk.size", String.valueOf((1024 * 32)));
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(6000));

		// 接收数据的地址和端口
		String zkQuorum = "10.67.1.181:2181";

		// 话题所在的组
		String group = "bsa_test";

		String topics = "topic3";

		// 每个话题的分片数
		int numThreads = 4;

		// 存放话题跟分片的映射关系
		Map<String, Integer> topicmap = new HashMap<>();
		topicmap.put(topics, numThreads);

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group,topicmap);

		JavaDStream<Person> lines = messages.map(new Function<Tuple2<String, String>, Person>() {
			public Person call(Tuple2<String, String> tuple2) {
				Person p = JSON.parseObject(tuple2._2(), Person.class);
				return p ;
			}
		});
		

		lines.foreachRDD(new VoidFunction<JavaRDD<Person>>() {

			@Override
			public void call(JavaRDD<Person> rdd) throws Exception {
				// TODO Auto-generated method stub
				DataFrame df = hc.createDataFrame(rdd, Person.class);

				String tempTableName = "person_tmp";

				String sql = "insert into table parquet_person partition(id) select name , age , id from person_tmp";

				df.registerTempTable(tempTableName);

				hc.sql(sql);
			}
		});
		
		jssc.start();
		
		jssc.awaitTermination();

	}

}


