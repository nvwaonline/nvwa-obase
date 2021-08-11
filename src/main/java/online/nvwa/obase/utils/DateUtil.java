package online.nvwa.obase.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    /**
     * 时间格式(yyyy-MM-dd)
     */
    public final static String DATE_PATTERN = "yyyy-MM-dd";
    /**
     * 时间格式(yyyy-MM-dd HH:mm:ss)
     */
    public final static String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static Date getDate() {
        return new DateTime().toDate();
    }

    public static String now() {
        return new DateTime().toString(DATE_TIME_PATTERN);
    }

    public static String format(Date date) {
        return format(date, DATE_PATTERN);
    }

    public static String format(Date date, String pattern) {
        if (date != null) {
            SimpleDateFormat df = new SimpleDateFormat(pattern);
            return df.format(date);
        }
        return null;
    }

    public static String startOfDay() {
        return new DateTime().toString("yyyy-MM-dd 00:00:00");
    }

    public static String endOfDay() {
        return new DateTime().toString("yyyy-MM-dd 23:59:59");
    }

    public static Date time2Date(String dateTime, String pattern) {
        DateTime time = DateTime.parse(dateTime, DateTimeFormat.forPattern(pattern));
        return time.toDate();
    }

    public static Date time2Date(String dateTime) {
        return time2Date(dateTime, DATE_TIME_PATTERN);
    }

    public static Date preDay() {
        return new DateTime().minusDays(1).withTime(0, 0, 0, 0).toDate();
    }


    /*
     * 将时间转换为时间戳
     */
    public static String dateToStamp(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }
    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }
    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s,String fmt){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fmt);
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }
    /**
     * 返回int类型的 年月
     *
     * @return YYYYMM
     */
    public static Integer monthAt() {
        String time = new DateTime().toString("yyyyMM");
        return Integer.valueOf(time);
    }

    public static DateTime time2DateTime(String dateTime, String pattern) {
        return DateTime.parse(dateTime, DateTimeFormat.forPattern(pattern));
    }

    public static void main(String[] args) {
        System.out.println(preDay());
    }


}


