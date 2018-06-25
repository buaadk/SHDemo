package com.test.storm.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.fastjson.JSONObject;
import com.test.kafka.KafkaProperties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends Thread {

	private Queue<String> queue = new ConcurrentLinkedQueue<String>();// 有序队列
	//private Queue<String> queue = new LinkedList<String>();// 有序队列
	
	private final String topic;

	public KafkaConsumer(String topic) {
		this.topic = topic;
	}

	private ConsumerConnector createConsumer() {
		Properties props = new Properties();
		// zookeeper 配置
		props.put("zookeeper.connect", KafkaProperties.brokerZkStr);
		props.put("group.id", KafkaProperties.groupId);

		// zk连接超时
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	@Override
	public void run() {
		ConsumerConnector consumer = createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		int i = 0;
		
		while (i<3 && iterator.hasNext()) {
			String message = new String(iterator.next().message());
			String addMsg = message;
			System.err.println("接收到: " + message);
			JSONObject json = JSONObject.parseObject(message);
			Object category = json.get("categoryid");
			Integer categoryid = Integer.parseInt(category + "");
			Object parent = json.get("parentid");
			Integer parentid = Integer.parseInt(parent + "");
			try {
				i++;
				queue.offer(addMsg);
				System.err.println("-------queue----------" + queue);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public Queue<String> getQueue() {
		return queue;
	}

}
