package com.ygsoft.transfer.es;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DBConnection {
	private static List <Connection> insertPool = new ArrayList<Connection>();
	
	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String ORACLE_URL = "jdbc:oracle:thin:@10.51.103.82:1521:PORTALVIEWER";
	private static final String USERNAME = "portal";
	private static final String PASSWORD = "portal";
	
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
