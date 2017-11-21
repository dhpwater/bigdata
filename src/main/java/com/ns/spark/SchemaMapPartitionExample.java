package com.ns.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

public class SchemaMapPartitionExample {

	private static KafkaCluster kafkaCluster = null;

	private static HashMap<String, String> kafkaParam = new HashMap<String, String>();

	private static Broadcast<HashMap<String, String>> kafkaParamBroadcast = null;

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("test");
		sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100") ;
		
		SparkContext sc = new SparkContext(sparkConf);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		final HiveContext hc = new HiveContext(sc);
		hc.setConf("parquet.memory.min.chunk.size", String.valueOf((1024 * 32)));
		
		// Create a StreamingContext with a 60 second batch size
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(60000));
		
        kafkaParamBroadcast = jssc.sparkContext().broadcast(kafkaParam);
        
		Set<String> topicSet = new HashSet<String>();
		topicSet.add("topic3");

		kafkaParam.put("metadata.broker.list", "10.67.1.181:9092");
		kafkaParam.put("group.id", "kafka.test002");

		// transform java Map to scala immutable.map
		scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParam);
		scala.collection.immutable.Map<String, String> scalaKafkaParam = testMap
				.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
					public Tuple2<String, String> apply(Tuple2<String, String> v1) {
						return v1;
					}
				});

		// init KafkaCluster
		kafkaCluster = new KafkaCluster(scalaKafkaParam);

		System.out.println("topicSet:"+topicSet);
		scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
		scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
		System.out.println("immutableTopics:"+immutableTopics);
		System.out.println(kafkaCluster.getPartitionMetadata(immutableTopics));
//		scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = kafkaCluster.getPartitions(immutableTopics).right().get();
		scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = (scala.collection.immutable.Set<TopicAndPartition>) kafkaCluster.getPartitions(immutableTopics).right().get();

		
		// kafka direct stream 初始化时使用的offset数据
		Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap<TopicAndPartition, Long>();

		// 没有保存offset时（该group首次消费时）, 各个partition offset 默认为0
		if (kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).isLeft()) {

			System.out.println(kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).left().get());

			Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

			for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
				consumerOffsetsLong.put(topicAndPartition, 0L);
			}

		} else {
			// offset已存在, 使用保存的offset
			scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster
					.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).right().get();
			
			Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

			Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

			for (TopicAndPartition topicAndPartition : topicAndPartitionSet1){
				 Long offset = (Long)consumerOffsets.get(topicAndPartition);
				 
	             consumerOffsetsLong.put(topicAndPartition, offset);
			}
		}
		
		//增加offset的有效性比较
		
        // create direct stream
        JavaInputDStream<String> message = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParam,
                consumerOffsetsLong,
                new Function<MessageAndMetadata<String, String>, String>() {
                    public String call(MessageAndMetadata<String, String> v1) throws Exception {
                        return v1.message();
                    }
                }
        );
        
        // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        
        JavaDStream<String> javaDStream = message.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd;
            }
        });
        
        
        JavaDStream<String> javaStreamFilterAgeRdd = javaDStream.filter(new Function<String, Boolean>() {
			
			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				JSONObject json = JSON.parseObject(v1);
				int age = json.getIntValue("age");
				return age > 18 ; 
			}
		}).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

			@Override
			public Iterable<String> call(Iterator<String> v1) throws Exception {
				// TODO Auto-generated method stub
				List<String> list = new ArrayList<String>();
				while (v1.hasNext())
				{
					JSONObject json = JSON.parseObject(v1.next());
				}
				return list;
			}
		});
        
    	List<StructField> fields = new ArrayList<StructField>();
    	fields.add(DataTypes.createStructField("id", DataTypes.LongType, true));
    	fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
    	fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
    	final StructType schema = DataTypes.createStructType(fields);
        
    	javaStreamFilterAgeRdd.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				
				// 数据入hive
				String tempTableName = "person_tmp";
				String sql = "insert into table parquet_person_002  select id , name , age  from person_tmp";
				DataFrame msgDF = hc.read().schema(schema).json(rdd);
				msgDF.registerTempTable(tempTableName);
				hc.sql(sql);
				
				//Update Topic Offset
				for (OffsetRange offsetRange : offsetRanges.get()) {
					// 封装topic.partition 与 offset对应关系 java Map
					TopicAndPartition topicAndPartition = new TopicAndPartition(offsetRange.topic(),
							offsetRange.partition());
					Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
					topicAndPartitionObjectMap.put(topicAndPartition, offsetRange.untilOffset());

					// 转换java map to scala immutable.map
					scala.collection.mutable.Map<TopicAndPartition, Object> topicAndPartitionMap = JavaConversions
							.mapAsScalaMap(topicAndPartitionObjectMap);
					
					scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap = topicAndPartitionMap
							.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
								public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
									return v1;
								}
							});
					
					kafkaCluster.setConsumerOffsets(kafkaParamBroadcast.getValue().get("group.id"), scalatopicAndPartitionObjectMap);
				}
			}
		});
		
		jssc.start();
		
		jssc.awaitTermination();
	}

}
