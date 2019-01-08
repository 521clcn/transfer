package com.ygsoft.transfer.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class TransferUtil{
	public static int preTotal = 10000;  //开始插入的记录数	
	public static int count = 1;
	public static long startTime = 0;
	public static int runTime = 0;
	public static int insertTime = 0;

	private static DataResultList  allList = new DataResultList();
	
	public static String sql;
	public static String retrievalSql;
	public static String insertSql;
	
	public void retrievalData() {
		Connection con = DBConnection.getConnection();

		try {
			PreparedStatement pstmt = con.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			int  total = 10000;
			while (rs.next()) {
				total = rs.getInt("count");
			}
			
			count = Runtime.getRuntime().availableProcessors()*2;
			if(count<1) {
				count =1;
				System.out.println("抽数线程数：------" + count);
				DataRetrievalThread thread = new DataRetrievalThread();
				thread.start();
			}else {				
				int preSize = total/count+1;

				System.out.println("抽数线程数：------" + count);
				for (int i = 0; i < count; i++) {
					DataRetrievalThread thread = new DataRetrievalThread(i, preSize);
					thread.start();
				}
			}


			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			if (con != null) {
				con.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void transferData(List list) {
		DataInsertThread insertThread = new DataInsertThread(list);
		insertThread.start();
	}

	/**
	 * @return the allList
	 */
	public static DataResultList getAllList() {
		return allList;
	}

	public static void main(String[] args) {
		startTime = System.currentTimeMillis();
		System.out.println("抽数开始-------------------");
		
		retrievalSql = " select k.id \"id\", k.registered_num \"registeredNum\", k.online_num \"onlineNum\", k.daily_login_num \"dailyLoginNum\", k.total_login_num \"totalLoginNum\", " + 
				"k.session_num \"sessionNum\", k.server_response_time \"serverResponseTime\", k.running_time \"runningTime\", k.table_space_size \"tableSpaceSize\", " + 
				"k.db_response_time \"dbResponseTime\", k.insert_date \"insertDate\", k.server_id \"serverId\" from pv_server_kpivalue k ";
		sql = " select count(1) count from ("+retrievalSql+")";
		
		new TransferUtil().retrievalData();
	}

}
