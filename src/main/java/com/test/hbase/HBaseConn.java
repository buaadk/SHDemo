package com.test.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConn {
	public static Configuration configuration;
	public static Connection connection;
	public static Admin admin;
	private static HBaseConfiguration hbaseConfig = null;

	// 初始化链接
	public static void init() {
		System.setProperty("hadoop.home.dir", "D:/data/hadoop-2.6.5");
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "master,slave2,slave3");
		configuration.set("hbase.master", "master:60010");
		try {
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 关闭连接
	public static void close() {
		try {
			if (null != admin)
				admin.close();
			if (null != connection)
				connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		try {
			init();
			listTables();//查询已有表
			// createTable("bjtuComputer", new String[] { "ctf1", "ctf2" });//
			// 创建新表
			// deleteTable("bjtuComputer");//删表
			String tableName = "SC";
			String row = "sc002";
			String[] fields = new String[] { "SC_Score:Math", "SC_Score:Computer Science", "SC_Score:English" };
			String[] values = new String[] { "80", "90", "100" };
			// addRecord(tableName, row, fields, values);

			String column = "SC_Score:Math";
			// String column = "SC_Score";
			scanColumn(tableName, column);
			String val ="100";
			//modifyData(tableName,row,column,val);
			 //deleteRow(tableName,row);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}

	// 查看已有表
	public static void listTables() throws Exception {

		HTableDescriptor[] hTableDescriptors = admin.listTables();
		for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
			System.err.println("----查询到的HBase列表为：-----" + hTableDescriptor.getNameAsString());
		}

	}

	// 建表
	public static void createTable(String tableNmae, String[] fields) throws IOException {

		TableName tableName = TableName.valueOf(tableNmae);

		if (admin.tableExists(tableName)) {
			System.err.println("talbe is exists!");
			// 删除已有表
			HBaseConn.deleteTable(tableName.getNameAsString());
		}
		// 创建表
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		for (String col : fields) {
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
			hTableDescriptor.addFamily(hColumnDescriptor);
		}
		admin.createTable(hTableDescriptor);
		System.err.println("新增Hbase表：" + tableName + "成功");

	}

	// 删表
	public static void deleteTable(String tableName) throws IOException {
		TableName tn = TableName.valueOf(tableName);
		if (admin.tableExists(tn)) {
			admin.disableTable(tn);
			admin.deleteTable(tn);
			System.err.println("删除Hbase表：" + tableName + "成功");
		}
	}

	// 添加信息
	public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		for (int i = 0; i != fields.length; i++) {
			Put put = new Put(row.getBytes());
			String[] cols = fields[i].split(":");
			put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
			table.put(put);
			System.err.println("--添加信息成功----表名：" + TableName.valueOf(tableName) + "----行为：" + row + "-------列值为:"
					+ (cols[0] + ":" + cols[1]) + "--值为：" + values[i]);
		}
		table.close();
	}

	// 浏览表tableName某一列的数据
	public static void scanColumn(String tableName, String column) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(column));
		/*
		 * ResultScanner scanner = table.getScanner(scan); for (Result result =
		 * scanner.next(); result != null; result = scanner.next()) {
		 * showCell(result); }
		 */
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			showCell(result);
		}
		table.close();
	}

	// 格式化输出
	public static void showCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
			System.out.println("Timetamp:" + cell.getTimestamp() + " ");
			System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
			System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
		}
	}
	//修改
	public static void modifyData(String tableName,String row,String column,String val)throws IOException{
	    Table table = connection.getTable(TableName.valueOf(tableName));
	    Put put = new Put(row.getBytes());
	    String[] str = column.split(":");
	    put.addColumn(str[0].getBytes(),str[1].getBytes(),val.getBytes());
	    table.put(put);
	    table.close();
	}
		
	public static void deleteRow(String tableName,String row)throws IOException{
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(row.getBytes());
	    //删除指定列族
	    //delete.addFamily(Bytes.toBytes(colFamily));
	    //删除指定列
	    //delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
		table.delete(delete);
		table.close();
	}

}
