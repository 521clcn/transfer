package com.ygsoft.transfer.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DBConnection {
	private static List <Connection> insertPool = new ArrayList<Connection>();
	
	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String ORACLE_URL = "jdbc:oracle:thin:@10.51.103.46:1521:ygbidw";
	private static final String USERNAME = "portalDemo";
	private static final String PASSWORD = "portalDemo";
	
	private static final String IORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String IORACLE_URL = "jdbc:oracle:thin:@10.51.103.46:1521:ygbidw";
	private static final String IUSERNAME = "portalDemo";
	private static final String IPASSWORD = "portalDemo";
	
//	private static final String IORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
//	private static final String IORACLE_URL = "jdbc:oracle:thin:@10.51.103.82:1521:PORTALVIEWER";
//	private static final String IUSERNAME = "portal";
//	private static final String IPASSWORD = "portal";
	
	public static Connection getConnection() {
		Connection con = null;
		try {
			Class.forName(ORACLE_DRIVER);
			con = DriverManager.getConnection(ORACLE_URL, USERNAME, PASSWORD);
		} catch (Exception e) {
			System.out.println("获取数据库连接失败，重新获取中...");		
			e.printStackTrace();
		}
		return con;
	}
	
//	public static Connection getInsertConnection() {
//		Connection con = null;
//		try {
//			Class.forName(IORACLE_DRIVER);
//			con = DriverManager.getConnection(IORACLE_URL, IUSERNAME, IPASSWORD);
//		} catch (Exception e) {
//			System.out.println("获取数据库连接失败，重新获取中...");
//			e.printStackTrace();
//		}
//
//		return con;
//	}

	
	public static Connection getInsertConnection() {
		Connection con = null;
		if(insertPool.size()>0) {
			con = insertPool.get(0);
			insertPool.remove(0);
		}
		if(con==null) {
			try {
				Class.forName(IORACLE_DRIVER);
				con = DriverManager.getConnection(IORACLE_URL, IUSERNAME, IPASSWORD);
			} catch (Exception e) {
				System.out.println("获取数据库连接失败，重新获取中...");		
				e.printStackTrace();
			}
		}

		return con;
	}
	
	public static void closeInsertConnection(Connection con) {
		if(insertPool.size()>30){
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}else {
			insertPool.add(con);
		}
	}
}
