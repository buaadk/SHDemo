package com.test.storm.service;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AreaFilterBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("categoryid", "parentid"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String order = input.getString(0);
		if (order != null) {
			String[] orderArr = order.split(",");
			collector.emit(new Values(orderArr[0], orderArr[1]));
			System.out.println("--------------》" + orderArr[0] + orderArr[1]);
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
