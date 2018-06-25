package com.test.storm.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.springframework.beans.factory.annotation.Autowired;

import com.test.storm.dao.HBaseDAO;

public class AreaRsltBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;
	Map<String, Object> countsMap = null;
	
	@Autowired
	private HBaseDAO dao;
	
	long beginTime = System.currentTimeMillis();
	long endTime = 0L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		countsMap = new HashMap<String, Object>();

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String date_areaid = input.getString(0);
		String order_amt = input.getString(1);
		countsMap.put(date_areaid, order_amt);
		endTime = System.currentTimeMillis();
		if (endTime - beginTime >= 5 * 1000) {
			for (String key : countsMap.keySet()) {
				// put into hbase
				// 这里把处理结果保存到hbase中
				dao.insert("area_order", key, "cf", "order_amt", countsMap.get(key) + "");
				System.err.println("rsltBolt put hbase: key=" + key + "; order_amt=" + countsMap.get(key));
			}
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
