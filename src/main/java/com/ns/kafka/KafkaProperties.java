package com.ns.kafka;

public class KafkaProperties {
    public static final String TOPIC = "person_top";
    public static final String BOOTSTRAP_SERVERS= "bsa63:9092,bsa64:9092";
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    public static final String TOPIC2 = "topic2";
    public static final String TOPIC3 = "topic3";
    public static final String CLIENT_ID = "my_test";
    private KafkaProperties() {}
}
