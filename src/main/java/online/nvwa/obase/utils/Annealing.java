package online.nvwa.obase.utils;

public class Annealing {
	public final static double FIRM_COEF = 1.0; //退火系数 永不磨灭
	public final static double RECM_COEF = 0.99; //退火系数 推荐
	private final static double PERIOD = 86400000.0; //周期为1天
	private final static long ORIGIN = 1451577600000L;  //时间原点设置为2016.01.01 1451577600000
//	private final static long ORIGIN = new Date("2016/01/01").getTime();  //时间原点设置为2016.01.01 1451577600000
	
	public static double anneValue(double value, double coef){
		if(coef == FIRM_COEF)return value;
		
		long past = System.currentTimeMillis() - ORIGIN;
		if(past <0)return value;
		
		return value*Math.pow(coef, past/PERIOD);
	}
	
	public static double unneValue(double value, double coef){
		if(coef == FIRM_COEF)return value;
		
		long past = System.currentTimeMillis() - ORIGIN;
		return value/Math.pow(coef, past/PERIOD);
	}
	
	/////////////////////////////////
	public static double add(double old, double inc, double coef){
		return old + unneValue(inc, coef );
	}
}
