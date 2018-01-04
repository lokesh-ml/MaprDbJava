package com.mastek.mapr.db;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class MapRDBUtils {
	private static Properties props = null;
	private static Configuration conf = null;
	private static HTable table = null;
	
	static {
		conf = HBaseConfiguration.create();
		// conf.set("hbase.table.namespace.mappings","t1:/,t15:/tables,t3:./,t20:/goose/tables");
		// conf.set("fs.default.name", "maprfs://xxx:7222");
//		conf.set("mapr.htable.impl", "com.mapr.fs.MapRHTable");
//		conf.set("hbase.zookeeper.quorum", "localhost");
//		conf.set("hbase.zookeeper.property.clientPort", "5181");
//		conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
		conf.set("hadoop.spoofed.user.uid", "5000");
//		conf.set("hadoop.spoofed.user.gid", "5000");
//		conf.set("hadoop.spoofed.user.username", "mapr");
	}
	
	public static HTable getTable(String tableName, Properties properties){
		try {
			table = new HTable(conf, tableName.getBytes());
			props = properties;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return table;
	}
	
	public static void createTable(String tableName, String[] columns){
        HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e1) {
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
        
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String column : columns) {
        	tableDescriptor.addFamily(new HColumnDescriptor(column));
		}
        
        try {
			admin.createTable(tableDescriptor);
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	@SuppressWarnings("deprecation")
	public static void scanDB(String value, String startKey, String endKey) {
		try {
			Scan s = new Scan();
			
			if (endKey != "") {
				s.addColumn(Bytes.toBytes(props.getProperty("columnFamily")), Bytes.toBytes(props.getProperty("columnQualifier")));
				s.setStartRow(Bytes.toBytes(startKey)); // start key is inclusive
				s.setStopRow(Bytes.toBytes(endKey));  // stop key is exclusive
				
			} else {
				Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					    new RegexStringComparator(startKey));
				s.setFilter(filter);
			}
			 
			ResultScanner ss = table.getScanner(s);
			System.out.println("scanner" + s);

			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.println("Row key found - " + new String(kv.getRow()));
//					System.out.print(new String(kv.getFamily()) + ":");
//					System.out.print(new String(kv.getQualifier()) + " ");
//					System.out.print(kv.getTimestamp() + " ");
//					System.out.println(new String(kv.getValue()));
					
					putRecord(new String(kv.getRow()), props.getProperty("columnFamily"), props.getProperty("columnQualifier"), value);
				}
			}
			
//			try {
//				for (Result rr = ss.next(); rr != null; rr = ss.next()) {
//					System.out.println("Found row: " + rr);
//				}
//			} finally {
//				ss.close();
//			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void putRecord(String rowKey, String columnFamily, String columnQualifier, String value) {
		Put p = new Put(Bytes.toBytes(rowKey));
		System.out.println("Updating Row Key - " + rowKey);
		p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(value));
		try {
			table.put(p);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void getRecord(String rowKey, String columnFamily, String columnQualifier){
		Get g = new Get(Bytes.toBytes(rowKey));
        Result r = null;
		try {
			r = table.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
        byte[] value = r.getValue(Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnQualifier));

        String valueStr = Bytes.toString(value);
        System.out.println("Read value from table and column family f1. Value is " + valueStr);
	}
}
