CREATE TABLE hbase_hive_1(key int, value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val") 
TBLPROPERTIES ("hbase.table.name" = "xyz");