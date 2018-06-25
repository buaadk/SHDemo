package com.test.storm.service;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class OrderBaseSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;
	Integer TaskId = null;
	SpoutOutputCollector collector = null;
	Queue<String> queue = new ConcurrentLinkedQueue<String>();

	String topic = null;

	public OrderBaseSpout(String topic) {
		this.topic = topic;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		TaskId = context.getThisTaskId();
		KafkaConsumer consumer = new KafkaConsumer(topic);
		consumer.start();
		queue = consumer.getQueue();

	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		if (queue.size() > 0) {
			String str = queue.poll();
			// 进行数据过滤
			System.err.println("TaskId:" + TaskId + ";  str=" + str);
			System.err.println("TaskId:" + TaskId + ";  str=" + str);
			System.err.println("TaskId:" + TaskId + ";  str=" + str);
			System.err.println("TaskId:" + TaskId + ";  str=" + str);
			System.err.println("TaskId:" + TaskId + ";  str=" + str);
			System.err.println("TaskId:" + TaskId + ";  str=" + str);
			collector.emit(new Values(str));
		}
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("order"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
