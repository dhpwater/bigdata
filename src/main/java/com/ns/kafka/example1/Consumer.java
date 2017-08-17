package com.ns.kafka.example1;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.alibaba.fastjson.JSON;
import com.ns.hive.Person;
import com.ns.kafka.KafkaProperties;

import kafka.utils.ShutdownableThread;

public class Consumer extends ShutdownableThread {

	private final KafkaConsumer<Integer, String> consumer;
	private final String topic;

	public Consumer(String topic) {
		super("KafkaConsumerExample", false);
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "70000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "80000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		consumer = new KafkaConsumer<>(props);
		this.topic = topic;
	}

	@Override
	public void doWork() {
		// TODO Auto-generated method stub
		consumer.subscribe(Collections.singletonList(this.topic));
		
		while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for (ConsumerRecord<Integer, String> record : records){
            	String val = record.value();
            	Person p = JSON.parseObject(val, Person.class);
            	System.out.println(p);
                System.out.printf("topic=%s, partition=%s, offset = %d, key = %s, value = %s\n", record.topic(),record.partition(),record.offset(), record.key(), record.value());
            }
        }
	}

	@Override
	public String name() {
		return null;
	}

	@Override
	public boolean isInterruptible() {
		return false;
	}

}
