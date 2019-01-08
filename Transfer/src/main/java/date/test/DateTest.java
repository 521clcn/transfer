package date.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DateTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");   
		try {
			Date date = formatter.parse("2018-09-13");
			List<Date> list = getThusdayDate(null);
			for(Date d : list) {
				System.out.println(formatter.format(d));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	private static List<Date> getThusdayDate(Date date){
		List<Date> list = new ArrayList<Date>();
		Date curDate = new Date();
		if(date==null) {
			date = curDate;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		
		do {
//			int num = calendar.get(Calendar.DAY_OF_WEEK);
//			if(num>5) {
				calendar.add(Calendar.WEEK_OF_YEAR,1);
				calendar.set(Calendar.DAY_OF_WEEK,5);
//			}else {
//				calendar.set(Calendar.DAY_OF_WEEK,5);
//			}
			Date belongDate = calendar.getTime();
			list.add(belongDate);
		}while(compareDate(calendar.getTime(),curDate));

		return list;
	}
	
	private static boolean compareDate(Date belongDate, Date curDate) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");   
		String belong = formatter.format(belongDate);
		String cur = formatter.format(curDate);
		try {
			Long start = formatter.parse(belong).getTime();
			Long end = formatter.parse(cur).getTime();
			if(start>=end) {
				return false;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return true;
	}

}
