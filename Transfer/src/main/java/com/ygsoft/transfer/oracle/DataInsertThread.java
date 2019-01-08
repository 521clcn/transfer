package com.ygsoft.transfer.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class DataInsertThread extends Thread {
	private List<Object> plist;
	
	public DataInsertThread(List<Object> plist) {
		this.plist = plist;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {	
		System.out.println(this.toString() +"插入数据开始: ");
//		System.out.println(this.toString() + "插入线程");
		String sql = TransferUtil.insertSql;
		try {
			Connection con = DBConnection.getInsertConnection();
			while(con==null) {
				try {
					System.out.println("Sleeping 10 seconds........");		
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				con = DBConnection.getInsertConnection();
			}
			
//			System.out.println(this.toString() + "插入线程;Connection:"+ con);
			long startTime = System.currentTimeMillis();
			
			con.setAutoCommit(false);
			PreparedStatement pstmt = con.prepareStatement(sql);						
//			int size = 0;
			
			int pindex = plist.size()-1;
			for (int i=pindex; i>=0; i--) {
				List<Object> obj = (List<Object>) plist.get(i);
				plist.remove(i);
//				System.out.println(sql);
//				System.out.println(obj);
				for(int j=0; j<obj.size(); j++) {
					int index = j+1;
					if(obj.get(j)!=null) {
						String postyj = obj.get(j).toString();
						if(postyj.length()>500) {
							System.out.println(postyj);
						}
					}

					pstmt.setObject(index, obj.get(j));
				}

				pstmt.addBatch();

//				size++;
//				if (size > 5000) {
//					pstmt.executeBatch();
//					con.commit();
//					pstmt.clearBatch();
//					size = 0;					
//				}
			}

			pstmt.executeBatch();
			con.commit();
			
			TransferUtil.insertTime++;
			long endTime = System.currentTimeMillis();
			System.out.println(this.toString()+"commit: "+(endTime-startTime)/(1000)+"秒---------------------"+(endTime-TransferUtil.startTime)/(1000)+"秒"+TransferUtil.insertTime);
			
			if (TransferUtil.runTime == TransferUtil.count) {
//				long endTime = System.currentTimeMillis();
				System.out.println(this.toString() + "插入线程耗时： " + (endTime - TransferUtil.startTime) / (1000) + " 秒");
			}
						
			if (pstmt != null) {
				pstmt.close();
			}
			if (con != null) {
//				con.close();	
				DBConnection.closeInsertConnection(con);
			}
		} catch (Exception e) {		
//			System.out.println(plist);
			DataInsertThread insertThread = new DataInsertThread(plist);
			insertThread.start();
			e.printStackTrace();
		}
	}
}
