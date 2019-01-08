import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test {
	public String getPreMonth(final String tday){
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");		
		Calendar calendar = Calendar.getInstance();
		try{
			Date date = format.parse(tday);
			calendar.setTime(date);
			calendar.add(Calendar.MONTH, -1);
			calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));

		}catch(Exception e){
			e.getMessage();
		}
		return format.format(calendar.getTime());
	}
	
	public String getPreYear(final String tday){
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");	
		Calendar calendar = Calendar.getInstance();
		try{
			Date date = format.parse(tday);
			calendar.setTime(date);
			calendar.add(Calendar.YEAR, -1);
			calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
		}catch(Exception e){
			e.getMessage();
		}
		return format.format(calendar.getTime());
	}
	
	private Date getDateByCertainDay(final String tday, final Integer day){
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");	
		Calendar calendar = Calendar.getInstance();
		try{
			Date date = format.parse(tday);		
			calendar.setTime(date);
			calendar.add(Calendar.DATE, day);
		}catch(Exception e){
			e.getMessage();
		}		
		return calendar.getTime();
	}

	public static void main(String[] args) throws ParseException {
		Calendar calendar1 = Calendar.getInstance();
		
		calendar1.setTime(new Date());
		calendar1.add(Calendar.WEEK_OF_YEAR,1);
		calendar1.set(Calendar.DAY_OF_WEEK,5);
		Date belongDate = calendar1.getTime();
		
		System.out.println(belongDate);
		
		
		// TODO Auto-generated method stub
		System.out.println(new Test().getPreYear("20180905"));
		System.out.println(new Test().getPreMonth("20180905"));
		
		Date expireDate = new Test().getDateByCertainDay("20181207",91);
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");	
    	String expireString = format.format(expireDate);
    	System.out.println(expireString);
		
		
		System.out.println(Double.valueOf("22.02").intValue());
		
		System.out.println(8>>1);
		System.out.println(8<<1);
		
    	String yearMonth = "2018-8";
    	
    	SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd");   
    	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM");   
        Date curData = formatter.parse(yearMonth);
        Calendar calendar = Calendar.getInstance();
		calendar.setTime(curData);
		calendar.set(Calendar.DAY_OF_MONTH,1);
		String curFirstDay = myFormat.format(calendar.getTime());
		System.out.println(curFirstDay);
		
		calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
		String curLastDay = myFormat.format(calendar.getTime());
		System.out.println(curLastDay);
		
		
		calendar.add(Calendar.MONTH,-1);
		calendar.set(Calendar.DAY_OF_MONTH,1);
		String preFirstDay = myFormat.format(calendar.getTime());
		System.out.println(preFirstDay);
		
		calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
		String preLastDay = myFormat.format(calendar.getTime());
		System.out.println(preLastDay);
		

	}

}
