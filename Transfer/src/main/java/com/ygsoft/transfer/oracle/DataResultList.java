package com.ygsoft.transfer.oracle;

import java.util.ArrayList;
import java.util.List;

public class DataResultList{
	private List<Object> list;
	
	public DataResultList() {
		list = new ArrayList<Object>();
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
	
	public List<Object> getList() {
		return list;
	}
	
	public int size() {
		return list.size();
	}

}
