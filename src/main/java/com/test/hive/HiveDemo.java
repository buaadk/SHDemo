package com.test.hive;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by liyuanqing on 2018/6/19.
 */
public class HiveDemo {
    private static JDBCTools jdbcTools;

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main( String[] args )
    {
        try {
            jdbcTools = new JDBCTools();
            jdbcTools.open("com.mysql.jdbc.Driver", "jdbc:mysql://192.168.56.122:3306/hive", "hive", "hive");
            Class.forName(driverName);
            Connection con = null;
            con = DriverManager.getConnection("jdbc:hive2://192.168.56.122:10000/default", "hive", "hive");
            Statement stmt = con.createStatement();
            ResultSet res = null;
            String sql = "select * from userbasic"; //show tables;
//            String sql = "show tables";
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            System.out.println("ok");
            while (res.next()) {
                String sql1 = "INSERT INTO userbasic " +
                        "VALUES ("+ "'" + res.getString(1)+ "'" + ","
                        +res.getInt(2)+ ","
                        +  res.getInt(3)+  ","
                        +  res.getFloat(4)+ ","
                        +  res.getFloat(5)+ ","
                        +  res.getFloat(6)+ ")";
                jdbcTools.executeInsert(sql1);
                System.out.println(res.getString(1) + "\t" + res.getInt(2) + "\t" );
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error");
        }
    }
}
