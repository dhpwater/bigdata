package com.ns.kafka.example1;

import com.ns.kafka.KafkaProperties;

public class KafkaProducerTest {

	public static void main(String[] args) {

		testProducer();
		
//		testConsumer();

	}
	
	public static void testProducer(){
		boolean isAsync = true;

		Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);

		producerThread.start();
	}
	
	public static void testConsumer(){
		Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);

		consumerThread.start();
	}
}
