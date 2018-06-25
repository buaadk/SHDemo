package com.test.storm.dao;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result; 
public interface HBaseDAO {
	public void save(Put put,String tableName) ;  
    public void insert(String tableName,String rowKey,String family,String quailifer,String value) ;  
    public void save(List<Put>Put ,String tableName) ;  
    public Result getOneRow(String tableName,String rowKey) ;  
    public List<Result> getRows(String tableName,String rowKeyLike) ;  
    public List<Result> getRows(String tableName, String rowKeyLike, String cols[]) ;  
    public List<Result> getRows(String tableName,String startRow,String stopRow) ;
}
