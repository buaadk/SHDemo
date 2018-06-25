package com.test.storm.controller;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import com.test.kafka.KafkaProperties;
import com.test.storm.service.AreaAmtBolt;
import com.test.storm.service.AreaFilterBolt;
import com.test.storm.service.OrderBaseSpout;
import com.test.storm.service.SpaFilterBolt;

public class MySelfTopology {

	public static void main(String[] args) {
		exec(args);
	}

	public static void exec(String[] args) {
		try {
			TopologyBuilder builder = new TopologyBuilder();
			OrderBaseSpout orderBaseSpout = new OrderBaseSpout(KafkaProperties.TOPIC);
			builder.setSpout("spout", orderBaseSpout, 1);
			builder.setBolt("filterblot", new AreaFilterBolt(), 1).shuffleGrouping("spout");
			builder.setBolt("amtbolt", new AreaAmtBolt(), 1).shuffleGrouping("filterblot");
			builder.setBolt("spa", new SpaFilterBolt(), 1).shuffleGrouping("amtbolt");
			//builder.setBolt("rsltolt", new AreaRsltBolt(), 1).shuffleGrouping("amtbolt");

			Config conf = new Config();
			conf.setDebug(false);
			if (args.length > 0) {
				try {
					StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology("mytopology", conf, builder.createTopology());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
