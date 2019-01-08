package com.ygsoft.transfer.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class TransferUtil{
	public static int preTotal = 5000;  //开始插入的记录数	
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
			
			count = Runtime.getRuntime().availableProcessors()/2;
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
		
		retrievalSql = " select t.oid, t.acttype, t.actid, t.activityid, t.processoid, t.processid, t.postid, t.yhdm, t.poststate, t.czcx, t.billgid, t.billid, t.sjdh, t.dh, t.year, t.preact, t.nextact, t.postyj, t.stime, t.etime, t.metaid, t.instancy, t.actstate, t.optype, t.wtyhdm, t.deptdxid from WF_ACTIVITIES t ";
		insertSql = " insert into wf_activities1 t (t.oid, t.acttype, t.actid, t.activityid, t.processoid, t.processid, t.postid, t.yhdm, t.poststate, t.czcx, t.billgid, t.billid, t.sjdh, t.dh, t.year, t.preact, t.nextact, t.postyj, t.stime, t.etime, t.metaid, t.instancy, t.actstate, t.optype, t.wtyhdm, t.deptdxid) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

		sql = " select count(*) count from ("+retrievalSql+")";
//		Runtime.getRuntime().availableProcessors();		
		
		new TransferUtil().retrievalData();
	}

}
