package online.nvwa.obase.data;

import online.nvwa.obase.utils.Base32;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;

//import org.apache.hadoop.hbase.util.Base64;
//import org.bouncycastle.util.Arrays;

public class Bytez extends Bytes{
	public static byte[] copy(byte[] bytes, int offset){
		return Arrays.copyOfRange(bytes, offset, bytes.length);
	}
	
	public static byte[] from(String v){
		return Bytes.toBytes(v);
	}
	public static byte[] from(boolean v){
		return Bytes.toBytes(v);
	}
	public static byte[] from(byte v){
		byte[] ret = new byte[1];
		ret[0] = v;
		return ret;
	}
	public static byte[] from(short v){
		return Bytes.toBytes(v);
	}
	public static byte[] from(int v){
		return Bytes.toBytes(v);
	}
	public static byte[] from(long v){
		return Bytes.toBytes(v);
	}
	public static byte[] from(float v){
		return Bytes.toBytes(v);
	}
	public static byte[] from(double v){
		return Bytes.toBytes(v);
	}
	public static byte[] from(Date v){
		return Bytes.toBytes(v.getTime());
	}
	public static byte[] from(Timestamp v){
		return Bytes.toBytes(v.getTime());
	}
	public static byte[] from(BigInteger v){
		return v.toByteArray();
	}
	public static String toBase64(byte[] v){
		return Base64.getEncoder().encodeToString(v);
	}
	public static byte[] fromBase64(String v){
		return Base64.getDecoder().decode(v);
	}
	
	public static String toBase32(byte[] v){
		return Base32.encode(v);
	}
	public static byte[] fromBase32(String v){
		return Base32.decode(v);
	}

	/**
	 * 8字节长度日期如： 2019-02-28 ==> 20190228
	 * @return
	 */
	public static String format(Date date){
		DateFormat df = new SimpleDateFormat("yyyyMMdd");
		return df.format(date);
	}
	public static String formatWithHour(Date date){
		DateFormat df = new SimpleDateFormat("yyyyMMddHH");
		return df.format(date);
	}
	public static Date dateFrom(String str){
		if(str!=null && str.length()==8 && str.indexOf("-")<0){
			str = str.substring(0,4)+"-"+str.substring(4,6)+"-"+str.substring(6,8);
		}

		DateFormat df = new SimpleDateFormat("yyyyMMdd");
		try {
			return df.parse(str);
		}catch (Exception e){
			e.printStackTrace();
			return null;
		}
//		return Date.valueOf(str);
	}

	/**
	 * 有一个问题。当V本身值已经为最大的时候，加一进位会导致位数增加，这个时候作为停止行不适合。
	 * @param v
	 * @return
	 */
	private final static byte[] nextSign = new BigInteger(new byte[128]).setBit(1024).toByteArray();
	static{
		for(int i=0; i<nextSign.length; i++)nextSign[i] |= 0xff;
	}
	public static byte[] next(byte[] v){
//		byte[] ret = new BigInteger(v).add(BigInteger.ONE).toByteArray();
//		if(ret.length > v.length){
//			ret = Bytez.add(v, nextSign);
//		}
//		return ret;
		return Bytez.add(v, nextSign);
	}

	public static byte[] max(byte[] v1, byte[] v2){
		return Bytez.compareTo(v1, v2)>0? v1:v2;
	}
	public static byte[] min(byte[] v1, byte[] v2){
		return Bytez.compareTo(v1, v2)>0? v2:v1;
	}
}
