package com.test.storm.dao.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import com.test.kafka.KafkaProperties;
import com.test.storm.dao.HBaseDAO;

public class HBaseDAOImp implements HBaseDAO {

	HConnection hTablePool = null;

	public HBaseDAOImp() {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", KafkaProperties.brokerZkStr);
		conf.set("hbase.rootdir", "hdfs://master/hbase");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		//conf.set("hbase.zookeeper.quorum", "master,slave2,slave3");
		try {
			hTablePool = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void save(Put put, String tableName) {
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(put);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void insert(String tableName, String rowKey, String family, String quailifer, String value) {
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			Put put = new Put(rowKey.getBytes());
			put.add(family.getBytes(), quailifer.getBytes(), value.getBytes());
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public void save(List<Put> Put, String tableName) {
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(Put);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public Result getOneRow(String tableName, String rowKey) {
		HTableInterface table = null;
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName);
			Get get = new Get(rowKey.getBytes());
			rsResult = table.get(get);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}

	@Override
	public List<Result> getRows(String tableName, String rowKeyLike) {
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rs : scanner) {
				list.add(rs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	@Override
	public List<Result> getRows(String tableName, String rowKeyLike, String[] cols) {
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			for (int i = 0; i < cols.length; i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes());
			}
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rs : scanner) {
				list.add(rs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	@Override
	public List<Result> getRows(String tableName, String startRow, String stopRow) {
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			Scan scan = new Scan();
			scan.setStartRow(startRow.getBytes());
			scan.setStopRow(stopRow.getBytes());
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rsResult : scanner) {
				list.add(rsResult);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

}
