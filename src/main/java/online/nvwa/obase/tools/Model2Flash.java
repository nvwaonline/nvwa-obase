package online.nvwa.obase.tools;

import java.io.IOException;
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
public class Model2Flash {
	public static Hashtable<String, String[]> types = new Hashtable<String, String[]>();
	static{
		types.put("boolean", new String[]{"Boolean","Boolean"});
		types.put("byte",  new String[]{"int","Byte"});
		types.put("short", new String[]{"int","Short"});
		types.put("int",  new String[]{"int","Int"});
		types.put("float", new String[]{"Number","Number"});
		types.put("double", new String[]{"Number","Double"});
		types.put("long", new String[]{"Number","Double"});
		
		types.put("Boolean", new String[]{"Boolean","Boolean"});
		types.put("Byte",  new String[]{"int","Byte"});
		types.put("Short", new String[]{"int","Short"});
		types.put("Integer", new String[]{"int","Int"});
		types.put("Float", new String[]{"Number","Number"});
		types.put("Double", new String[]{"Number","Double"});
		types.put("Long", new String[]{"Number","Double"});
		types.put("byte[]", new String[]{"ByteArray","Bytes"});
		types.put("String", new String[]{"String","String"});		
		types.put("NGAData", new String[]{"NGAData","Data"});		
	}
	
	public static void main(String[] args) throws IOException {
//		String source = model2entity(Blog.class);
//		System.out.print(source);
	}
	
	public static String model2entity(Class<?> c){
		Field[] fields = c.getDeclaredFields(); 

		PrintBuffer sb = new PrintBuffer();

		sb.println("package "+ c.getPackage().getName()+"{");

		sb.println("import flash.utils.ByteArray;");
		sb.println();
		sb.println("[Bindable]");
		sb.println("public class T" + c.getSimpleName() + "{");
		
		for(Field f : fields){
			String type = f.getType().getSimpleName();
			String name = f.getName();
			sb.println("public var " + name.substring(0, 1).toUpperCase()+name.substring(1) +":"+types.get(type)[0]+";");
		}		
		sb.println();
		
		sb.println("public static function readFrom(p:NGAData):T" + c.getSimpleName() + "{");
		sb.println("var item:T" + c.getSimpleName() +" = new T" + c.getSimpleName()+"();");
		try {
			if(c.getDeclaredField("row")!=null){
			sb.println("item.Row = p.readBytes();");
			}
		} catch (Exception e) {
		}
		try {
			if(c.getDeclaredField("createTime")!=null){
				sb.println("item.CreateTime = p.readDouble();");
			}
		} catch (Exception e) {
		}
		
		
		for(Field f : fields){
			if(f.getName().equals("row"))
				continue;
			if(f.getName().equals("createTime"))
				continue;
			String type = f.getType().getSimpleName();
			String name = f.getName();
			
			sb.println("item."+name.substring(0, 1).toUpperCase()+name.substring(1)+" = p.read"+types.get(type)[1]+ "();");
		}
		sb.println();
		sb.println("return item;");
		sb.println("}");
		

	


		sb.println("}");
		sb.println("}");

		return sb.toString();		
		
	}

	public static String field2as(Field f){
		String type = f.getType().getSimpleName();
		String name = f.getName();
		
		PrintBuffer sb = new PrintBuffer();

		sb.println("/**");
		sb.println("*"+type+" "+name);
		sb.println("*/");
	
		sb.println("public void set"+name.substring(0, 1).toUpperCase()+name.substring(1)+"("+type + " value){");
		if("byte[]".equals(type))
			sb.println("this.setAttribute(\""+name+"\", value);");
		else
			sb.println("this.setAttribute(\""+name+"\", Bytez.from(value));");
		sb.println("}");
		
		sb.println("public "+type + " get"+name.substring(0, 1).toUpperCase()+name.substring(1)+"(){");
		sb.println("byte[] bytes = this.getAttribute(\""+name+"\");");
		sb.println("if(bytes == null)");
		switch(type){
		case "String":
			sb.println("return \"\";");
			break;
		case "byte[]":
			sb.println("return null;");
			break;
		default:
			sb.println("return 0;");
		}

		sb.println("return Bytez.to"+type.substring(0, 1).toUpperCase()+type.substring(1)+"(bytes);");
	
		sb.println("}");
		

		return sb.toString();
	}
}
