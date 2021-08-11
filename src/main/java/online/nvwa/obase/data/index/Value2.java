package online.nvwa.obase.data.index;

import online.nvwa.obase.data.Bytez;

public class Value2{
	public final static Value2 EXIST = Value2.from(Integer.MAX_VALUE); //存在
	public final static Value2 NOT_EXIST = Value2.from(0);//不存在
	
	public final static int Type_Unknown = 0;
	public final static int Type_Boolean = 1;
	public final static int Type_Byte = 2;
	public final static int Type_Short = 3;
	public final static int Type_Int = 4;
	public final static int Type_Long = 5;
	public final static int Type_Float = 6;
	public final static int Type_Double = 7;
	public final static int Type_String = 8;
	public final static int Type_Bytes = 9;
	
	private int type;
	private byte[] data;
	
	public Value2(int type, byte[] data){
		this.type = type;
		this.data = data;
	}
	
	public byte[] encode(){
		return Bytez.add(Bytez.from(type), data);
	}
	public static Value2 decode(byte[] bytes){
		if(bytes == null)return null;
		return new Value2(Bytez.toInt(bytes), Bytez.tail(bytes, bytes.length-4));
	}
	
	public boolean equal(Value2 v){
		if(this == v)return true;
		if(v != null && this.type == v.type && Bytez.compareTo(this.getData(),v.getData())==0)
			return true;
		return false;
	}
	
	public static Value2 from(String v){
		return new Value2(Type_String, Bytez.from(v));
	}
	
	public static Value2 from(boolean v){
		return new Value2(Type_Boolean, Bytez.from(v));
	}
	public static Value2 from(byte v){
		return new Value2(Type_Byte, Bytez.from(v));
	}
	public static Value2 from(short v){
		return new Value2(Type_Short, Bytez.from(v));
	}
	public static Value2 from(int v){
		return new Value2(Type_Int, Bytez.from(v));
	}
	public static Value2 from(long v){
		return new Value2(Type_Long, Bytez.from(v));
	}
	public static Value2 from(float v){
		return new Value2(Type_Float, Bytez.from(v));
	}
	public static Value2 from(double v){
		return new Value2(Type_Double, Bytez.from(v));
	}
	public static Value2 from(byte[] v){
		return new Value2(Type_Bytes, v);
	}
	
	public boolean isString(){
		return this.type == Value2.Type_String;
	}
	public boolean isBoolean(){
		return this.type == Value2.Type_Boolean;
	}
	public boolean isNumber(){
		return this.type == Value2.Type_Double;
	}
	
	public boolean asBoolean(){
		return Bytez.toBoolean(this.getData());
	}
	public double asNumber(){
		return Bytez.toDouble(this.getData());
	}
	public String asString(){
		return Bytez.toString(this.getData());
	}
	
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
	
	/**
	 * 排序的字节数组
	 * @return
	 */
	public byte[] ranking(){
		return ranking(data, type);
	}

	private static byte[] ranking(byte[] data, int type){
		switch(type){
		//固定长度
		case Type_Boolean:
			return data.clone();
		
		//固定符号
		case Type_Byte:
		case Type_Short:
		case Type_Int:
		case Type_Long:
		{
			byte sign = data[0];
			if(sign >=0)
				return Bytez.add(Bytez.from(true), data);
			else
				return Bytez.add(Bytez.from(false), data);
		}
		case Type_Float:
		case Type_Double:
		{
			byte sign = data[0];

			if(sign >=0)
				return Bytez.add(Bytez.from(true), data);
			else{
				byte[] key = data.clone();
				for(int j=0; j<key.length; j++){
					key[j] = (byte)~key[j];
				}
				return Bytez.add(Bytez.from(false), key);
			}
		}

		//可变长度
		case Type_String:
		case Type_Bytes:
			return Bytez.add(Bytez.from((short)data.length), data);
		}
		
		return data;
	}
	
	public Number toNumber(){
		switch(type){
		case Type_Byte:
		case Type_Short:
			return  Bytez.toShort(data);		
		case Type_Int:
			return Bytez.toInt(data);		
		case Type_Long:
			return Bytez.toLong(data);		
		case Type_Float:
			return Bytez.toFloat(data);		
		case Type_Double:
			return Bytez.toDouble(data);				
		}		
		return null;
	}
	

	private static byte[] nullData(int type){
		switch(type){
		//固定长度
		case Type_Boolean:
			return Bytez.from(false);
		
		//固定符号
		case Type_Byte:
			return Bytez.from((byte)0);
		case Type_Short:
			return Bytez.from((short)0);
		case Type_Int:
			return Bytez.from((int)0);
		case Type_Long:
			return Bytez.from((long)0);
		case Type_Float:
			return Bytez.from((float)0);
		case Type_Double:
			return Bytez.from((double)0);
		case Type_String:
			return Bytez.from("");
		case Type_Bytes:
			return new byte[0];
		}
		
		return null;
	}
	
	public static boolean inScope(Value2 start, Value2 stop, byte[] value){
		if(value == null)value = nullData(start.type);
		
		byte[] ranking = ranking(value, start.type);
		if(Bytez.compareTo(start.ranking(), ranking)>0)
			return false;
		if(Bytez.compareTo(stop.ranking(), ranking)<=0)
			return false;
		
		return true;
	}
	
	public String toString(){
		if(data == null)data = Value2.nullData(type);
		
		switch(type){
		case Type_Boolean:
			return Bytez.toBoolean(data)+"";		
		case Type_Byte:
		case Type_Short:
			return Bytez.toShort(data)+"";		
		case Type_Int:
			return Bytez.toInt(data)+"";		
		case Type_Long:
			return Bytez.toLong(data)+"";		
		case Type_Float:
			return Bytez.toFloat(data)+"";		
		case Type_Double:
			return Bytez.toDouble(data)+"";		

		//可变长度
		case Type_String:
			return Bytez.toString(data);		
		case Type_Bytes:
			return Bytez.toBase64(data);
		}
		
		return "";
	}
}
