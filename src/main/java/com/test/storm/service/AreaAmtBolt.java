package com.test.storm.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.test.hbase.HBaseConn;
import com.test.storm.dao.HBaseDAO;
import com.test.storm.dao.impl.HBaseDAOImp;
import com.test.util.DateFmt;

public class AreaAmtBolt implements IBasicBolt {
	private static final long serialVersionUID = 1L;
	Map<String, Object> countsMap = null;
	String today = null;
	HBaseDAO dao = null;

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
		countsMap = new HashMap<String, Object>();
		dao = new HBaseDAOImp();
		// 根据hbase里初始值进行初始化countsMap
		today = DateFmt.getCountDate(null, DateFmt.date_short);

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input != null) {
			
			try {
				String categoryid = input.getStringByField("categoryid");
				String parentid = input.getStringByField("parentid");
				
				today = DateFmt.getCountDate(null, DateFmt.date_short);
				String[] cols = new String[] {categoryid, parentid};
				HBaseConn.init();
				HBaseConn.listTables();
				
				countsMap.put(categoryid,parentid);
				System.err.println("categoryid:" + categoryid + "parentid:" + parentid);
				collector.emit(new Values(categoryid + "_" + parentid));

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	@Override
	public void cleanup() {
		countsMap.clear();
	}

	/*public Map<String, Object> initMap(String rowKeyDate, HBaseDAO dao, String[] cols) {
		Map<String, Object> countsMap = new HashMap<String, Object>();
		List<Result> list = dao.getRows(HBaseProperties.tableName, rowKeyDate, cols);
		for (Result rsResult : list) {
			String rowKey = new String(rsResult.getRow());
			for (KeyValue keyValue : rsResult.raw()) {
				if (cols.equals(new String(keyValue.getQualifier()))) {
					countsMap.put(rowKey, Double.parseDouble(new String(keyValue.getValue())));
					break;
				}
			}
		}

		return countsMap;

	}*/

}
