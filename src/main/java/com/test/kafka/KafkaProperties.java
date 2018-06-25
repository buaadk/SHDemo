package com.test.kafka;

public class KafkaProperties {
	public static final String TOPIC = "Ex3";

	public static final String brokerZkStr = "192.168.56.121:2181,192.168.56.122:2181,192.168.56.123:2181";

	public static final String groupId = "MyTrack19";// spout的唯一标志

	public static final String zkRoot = "/kafka";// 用来保存消费者的偏离值

	public static String brokerZkPath = "/kafka/brokers";
}
