package com.ygsoft.transfer.es;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataResultList{
	private List<Map<String,Object>> list;
	
	public DataResultList() {
		list = new ArrayList<Map<String,Object>>();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void addAll(List c) {
		synchronized(DataResultList.class) {			
			list.addAll(c);
		
			if(list.size()>TransferUtil.preTotal) {
				List<Object> plist = new ArrayList<Object>();
				plist.addAll(list);
				list.clear();			
				new TransferUtil().transferData(plist);					
			}
			if (TransferUtil.runTime == TransferUtil.count) {
				List<Object> plist = new ArrayList<Object>();
				plist.addAll(list);
				list.clear();
				new TransferUtil().transferData(plist);
			}
		}
	}
	
	public void clear() {
		list.clear();
	}
	
	public List<Map<String,Object>> getList() {
		return list;
	}
	
	public int size() {
		return list.size();
	}

}
