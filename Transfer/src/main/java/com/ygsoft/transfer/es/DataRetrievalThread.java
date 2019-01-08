package com.ygsoft.transfer.es;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataRetrievalThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataRetrievalThread.class);
	private int index;
	private int size;
	private List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

	public DataRetrievalThread(int index, int size) {
		this.index = index;
		this.size = size;
	}

	public DataRetrievalThread() {
	}

	@Override
	public void run() {
		LOGGER.info(this.toString() + "取数线程，取数开始-------------------" + this.toString() + "		" + (index + 1));

		try {
			String sql_i = " select * from (select t_t.*, rownum rn from (" + TransferUtil.retrievalSql + ") t_t where rownum <=?) where rn>? ";
			Connection con = DBConnection.getConnection();

			while (con == null) {
				try {
					LOGGER.info("Sleeping 5 seconds........");
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				con = DBConnection.getConnection();
			}

			LOGGER.info(this.toString() + "取数线程;Connection:" + con);
			PreparedStatement pstmt_i = con.prepareStatement(sql_i);
			pstmt_i.setInt(1, size * (index + 1));
			pstmt_i.setInt(2, size * index);

			ResultSet rs_i = pstmt_i.executeQuery();

			ResultSetMetaData metaData = rs_i.getMetaData();

			int colCount = metaData.getColumnCount() - 1;

			while (rs_i.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < colCount; i++) {
					String colName = metaData.getColumnName(i + 1);
					Object obj = rs_i.getObject(i + 1);
					if (obj instanceof Date) {
						Timestamp ts = rs_i.getTimestamp(colName);
						Date date = new Date(ts.getTime());
						SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//						Date day = format.parse(format.format(date));
						map.put(colName, date);
						map.put("insertDay", format.format(date));
					} else if (obj instanceof BigDecimal) {
						BigDecimal bd = rs_i.getBigDecimal(colName);
						map.put(colName, bd.longValue());
					} else {
						map.put(colName, rs_i.getObject(i + 1));
					}

				}
				list.add(map);

				if (list.size() > TransferUtil.preTotal) {
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
				LOGGER.info("取数完成-----取数耗时-------------------" + (start - TransferUtil.startTime) / 1000 + "秒");
			}

			long end = System.currentTimeMillis();
			LOGGER.info(this.toString() + "取数线程取数耗时-------------------" + (end - TransferUtil.startTime) / 1000
					+ "秒" + TransferUtil.runTime);

			if (rs_i != null) {
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
