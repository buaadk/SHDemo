package com.test.kafka.producer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSONObject;
import com.test.kafka.service.EventService;
import com.test.kafka.vo.CategoryTree;
import com.test.kafka.vo.Event;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

@RestController
@RequestMapping("/mulconsumer")
@EnableScheduling
public class MulConsumerController {
	private static kafka.javaapi.consumer.ConsumerConnector consumer;

	@Autowired
	private EventService eventService;

	private static final String TOPIC = "multipleSource";
	
	@Scheduled(fixedRate=5000)
	public void timeExec() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		System.out.println("每隔5秒执行一次 "+dateFormat.format(new Date()));
	}
	
	//@Scheduled(fixedRate=30000)
	public void consume() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		System.out.println("每隔30秒执行一次 "+dateFormat.format(new Date()));
		
		Properties props = new Properties();
		// zookeeper 配置
		// props.put("zookeeper.connect", "cluster1:2181");
		props.put("zookeeper.connect", "192.168.56.121:2181,192.168.56.122:2181,192.168.56.123:2181");

		// group 代表一个消费组
		props.put("group.id", "group2");

		// zk连接超时
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ConsumerConfig config = new ConsumerConfig(props);

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));

		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(TOPIC).get(0);// 获取每次接收到的这个数据
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		List<CategoryTree> treeList = new ArrayList<>();
		while (iterator.hasNext()) {
			String message = new String(iterator.next().message());
			System.out.println("接收到: " + message);
			JSONObject json = JSONObject.parseObject(message);
			
			Event event = new Event();
			Object stamp = json.get("timestamp");
			String timestamp = stamp + "";
			Date date = null;
			try {
				long l = new Long(timestamp);
				date = new Date(l);
				event.setTimestamp(date);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			Object id = json.get("visitorid");
			Integer visitorid = Integer.parseInt(id + "");
			event.setVisitorid(visitorid);
			
			Object eve = json.get("event");
			String eventVal = eve + "";
			event.setEvent(eventVal);
			
			Object ite = json.get("itemid");
			Integer itemid = Integer.parseInt(ite + "");
			event.setItemid(itemid);
			
			Object transaction = json.get("transactionid");
			String transactionid = transaction + "";
			event.setTransactionid(transactionid);
			
			System.out.println("--------begin----save------");
			try {
				eventService.save(event);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

}
