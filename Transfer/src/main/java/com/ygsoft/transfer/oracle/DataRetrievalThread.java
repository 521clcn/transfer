package com.ygsoft.transfer.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

public class DataRetrievalThread extends Thread {
	private int index;
	private int size;
	private List<List<Object>> list = new ArrayList<List<Object>>();

	public DataRetrievalThread(int index, int size) {
		this.index = index;
		this.size = size;
	}
	
	public DataRetrievalThread() {}

	@Override
	public void run() {
		System.out.println(this.toString() + "取数线程，取数开始-------------------" + this.toString()+"		"+(index + 1));

		if(TransferUtil.count==1) {
			try {
				Connection con = DBConnection.getConnection();
				while(con==null) {
					try {
						System.out.println("Sleeping 5 seconds........");	
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					con = DBConnection.getConnection();
				}
				
				PreparedStatement pstmt_i = con.prepareStatement(TransferUtil.retrievalSql);
				ResultSet rs_i = pstmt_i.executeQuery();
				
				ResultSetMetaData metaData = rs_i.getMetaData();
				
				int colCount = metaData.getColumnCount();			
				
				while (rs_i.next()) {	
					List<Object> obj = new ArrayList<Object>();
					for(int i=0; i<colCount; i++) {
						obj.add(i, rs_i.getObject(i+1));
					}
					list.add(obj);

					if(list.size()>10000) {
						TransferUtil.getAllList().addAll(list);
						list.clear();
					}
				}
				TransferUtil.runTime++;		
				TransferUtil.getAllList().addAll(list);
				list.clear();
				
				long end = System.currentTimeMillis();
				System.out.println(this.toString() + "取数线程取数耗时-------------------" + (end - TransferUtil.startTime) / 1000 + "秒"+TransferUtil.runTime);
				
				if(rs_i !=null) {
					rs_i.close();
				}
				if (pstmt_i != null) {
					pstmt_i.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}else {
			try {
				String sql_i = " select * from (select t_t.*, rownum rn from ("+TransferUtil.retrievalSql + ") t_t where rownum <=?) where rn>? ";
				Connection con = DBConnection.getConnection();
				
				while(con==null) {
					try {
						System.out.println("Sleeping 5 seconds........");	
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					con = DBConnection.getConnection();
				}
				
				System.out.println(this.toString() + "取数线程;Connection:"+ con);
				PreparedStatement pstmt_i = con.prepareStatement(sql_i);
				pstmt_i.setInt(1, size * (index + 1));
				pstmt_i.setInt(2, size * index );
				
				ResultSet rs_i = pstmt_i.executeQuery();
				
				ResultSetMetaData metaData = rs_i.getMetaData();
				
				int colCount = metaData.getColumnCount()-1;			
				
				while (rs_i.next()) {	
					List<Object> obj = new ArrayList<Object>();
					for(int i=0; i<colCount; i++) {
						obj.add(i, rs_i.getObject(i+1));
					}
					list.add(obj);

					if(list.size()>10000) {
						TransferUtil.getAllList().addAll(list);
						list.clear();
					}
				}
										
				TransferUtil.getAllList().addAll(list);
				list.clear();
				
				TransferUtil.runTime++;	
				TransferUtil.getAllList().addAll(list);

				if (TransferUtil.runTime == TransferUtil.count) {
					long start = System.currentTimeMillis();
					System.out.println("取数完成-----取数耗时-------------------" + (start - TransferUtil.startTime) / 1000 + "秒");
				}
				
				long end = System.currentTimeMillis();
				System.out.println(this.toString() + "取数线程取数耗时-------------------" + (end - TransferUtil.startTime) / 1000 + "秒"+TransferUtil.runTime);
				
				if(rs_i !=null) {
					rs_i.close();
				}
				if (pstmt_i != null) {
					pstmt_i.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		

	}
	

}
