package online.nvwa.obase.tools;

import java.lang.reflect.Field;
import java.util.Hashtable;

/**
 * 通过模型定义，自动生成什么文件？框架代码
 * 1. 生成Remote接口？
 * 2. 生成数据库访问文件；Entity
 * 3. 生成代理访问文件？Proxy
 * 4. 生成客户端对象文件；
 * 5. 生成序列化代码；
 */
public class Entity2Hbase {
	public static Hashtable<String, String[]> types = new Hashtable<String, String[]>();
	static{
		types.put("boolean", new String[]{"Boolean","boolean"});
		types.put("byte",  new String[]{"Byte","byte"});
		types.put("short", new String[]{"Short","short"});
		types.put("int",  new String[]{"Int","int"});
		types.put("float", new String[]{"Float","float"});
		types.put("double", new String[]{"Double","double"});
		types.put("long", new String[]{"Long","long"});
		
		types.put("Boolean", new String[]{"Boolean","boolean"});
		types.put("Byte",  new String[]{"Byte","byte"});
		types.put("Short", new String[]{"Short","short"});
		types.put("Integer", new String[]{"Int","int"});
		types.put("Float", new String[]{"Float","float"});
		types.put("Double", new String[]{"Double","double"});
		types.put("Long", new String[]{"Long","long"});
		types.put("byte[]", new String[]{"Bytes","Bytes"});
		types.put("String", new String[]{"String","String"});
		types.put("Date", new String[]{"Date","Date"});
		types.put("Timestamp", new String[]{"Timestamp","Timestamp"});
		types.put("BigInteger", new String[]{"Long","long"});


	}
	
//	public static void main(String[] args) throws IOException {
//		String source = model2entity(Product.class);
//		System.out.print(source);
//	}
	
	public static String model2entity(Class<?> c){
		Field[] fields = c.getDeclaredFields(); 

		PrintBuffer sb = new PrintBuffer();

		sb.println("package com.biocloo.bic.biz.hbase.tables;");
		sb.println();

		sb.println("import org.apache.hadoop.hbase.client.Result;");
		
		sb.println("import online.nvwa.obase.data.Bytez;");
		sb.println("import online.nvwa.obase.data.TimeEntity;");

		boolean importDate = false;
		boolean importTimestamp = false;
		for(Field f : fields){
			String type = f.getType().getSimpleName();
			switch (type){
				case "Date":
					importDate = true;
					break;
				case "Timestamp":
					importTimestamp = true;
					break;
			}
		}
		if(importDate)sb.println("import java.sql.Date;");
		if(importTimestamp)sb.println("import java.sql.Timestamp;");


		sb.println();
		sb.println("public class T" + c.getSimpleName() + " extends TimeEntity{");

		sb.println("protected final static String tableName = \""+c.getSimpleName().toLowerCase()+"\";");
		sb.println();

		sb.println("public T"+c.getSimpleName()+"(byte[] row) {");
		sb.println("super(row);");
		sb.println("}");

		sb.println("public T"+c.getSimpleName()+"(Result r) {");
		sb.println("super(r);");
		sb.println("}");

		for(Field f : fields){
			if(f.getName().equals("serialVersionUID"))
				continue;

			sb.println(field2as(f));
 		}

		sb.println("}");

		return sb.toString();		
		
	}

	public static String field2as(Field f){
		try {
			String type = f.getType().getSimpleName();
			String name = f.getName();

			PrintBuffer sb = new PrintBuffer();

			sb.println("/**");
			sb.println("*" + types.get(type)[1] + " " + name);
			sb.println("*/");

			sb.println("public void set" + name.substring(0, 1).toUpperCase() + name.substring(1) + "(" + types.get(type)[1] + " value){");
			if ("byte[]".equals(type))
				sb.println("this.setAttribute(\"" + name + "\", value);");
			else if ("String".equals(type)) {
				sb.println("if(value==null)value=\"\";");
				sb.println("this.setAttribute(\"" + name + "\", Bytez.from(value));");
			} else
				sb.println("this.setAttribute(\"" + name + "\", Bytez.from(value));");
			sb.println("}");

			sb.println("public " + types.get(type)[1] + " get" + name.substring(0, 1).toUpperCase() + name.substring(1) + "(){");
			sb.println("return this.getAttributeAs" + types.get(type)[0] + "(\"" + name + "\");");
			sb.println("}");


			return sb.toString();
		}catch (Exception e){
			System.out.println(e);
			System.out.println(f.toString());

			return null;
		}
	}
}
