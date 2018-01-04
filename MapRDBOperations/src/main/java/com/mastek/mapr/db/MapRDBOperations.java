package com.mastek.mapr.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class MapRDBOperations {
	
	public static void main(String[] args) {
		System.out.println("Starting...");
		
		Properties props = new Properties();
		try {
			props.load(new FileInputStream("./conf.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String tableName = props.getProperty("maprTableName");
		System.out.println("tableName " + tableName);
		
		MapRDBUtils.getTable(tableName, props);
		XlsUtils.readExcel(props);
		
		System.out.println("Done");
	}
}
