package online.nvwa.obase.tools;

import javassist.*;
import javassist.bytecode.CodeAttribute;
import javassist.bytecode.LocalVariableAttribute;
import javassist.bytecode.MethodInfo;
import online.nvwa.obase.tools.FuncScope.Scope;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Hashtable;

public class Java2Flash {
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
	
//	public static void main(String[] args) throws IOException {
//		String source = class2as(CommUser.class);
//		System.out.println(source);
//	}

	public static String class2as(Class<?> c){		
		StringBuffer sb = new StringBuffer();
		sb.append(c.getPackage().toString()+"{\n");

		sb.append("import flash.utils.ByteArray;\n");

		sb.append("import cc.jiamen.network.NGAPackage;\n");
		sb.append("import cc.jiamen.network.Protocol;\n");
		sb.append("import cc.jiamen.views.MessageBox;\n");

		sb.append("public class " + c.getSimpleName() + "{\n");

		Method[] ms = c.getMethods();

		for(Method m : ms){
            if(m.isAnnotationPresent(FuncScope.class)){
            	FuncScope funcScope = (FuncScope) m.getAnnotation(FuncScope.class);
            	if(funcScope.value() == Scope.APPLICATION){
        			sb.append(method2as(m.getDeclaringClass(), m));
            	}
            }
            
//            System.out.println(m.getName()+":"+m.getReturnType().getSimpleName());
		}

		sb.append("}\n");
		sb.append("}");

		return sb.toString();		
	}

	public static String method2as(Class<?> clazz, Method m){
		String type = m.getReturnType().getSimpleName();
		
		String[] paraNames = getMethodParamNames(clazz, m);
		PrintBuffer sb = new PrintBuffer();
		sb.println("/**");
		sb.println("*Return Type: " + type); 
		sb.println("*/");
		sb.print("public static function ");
		sb.print(m.getName());
		sb.print("(");
		int i = 0;
		for(Parameter p : m.getParameters()){
			if(i>0)sb.print(", ");
			sb.print(paraNames[i] + ":" + types.get(p.getType().getSimpleName())[0]);
			i++;
		}
		if(i>0)sb.print(", ");
		sb.print("callback:Function=null");
		sb.print(")");
		sb.print(":void");
		sb.println("{");


		sb.println("var p:NGAPackage = new NGAPackage(Protocol.RemoteCall);");
		sb.print("p.writeString(\"");
		sb.print(m.getName());
		sb.println("\");");

		sb.print("p.writeInt(");
		sb.print(""+m.getParameterTypes().length);
		sb.println(");");

		i = 0;
		for(Parameter p : m.getParameters()){
			sb.print("p.writeString(");
			sb.print("\""+p.getType().getSimpleName()+"\"");
			sb.println(");");

			sb.print("p.write"+types.get(p.getType().getSimpleName())[1]+"(");
			sb.print(paraNames[i]);
			sb.println(");");
			i++;
		}
		
		String readResult = "";
		if(types.containsKey(type)){
			readResult = "r.read" + types.get(type)[1]+"()";
		}

		sb.println("Center.server.sendRequest(p, function(r:NGAPackage):void{");
		sb.println("if(r.readBoolean()){");
//		sb.println("if(callback!=null)callback(r.readData());");
		sb.println("if(callback!=null)callback("+readResult+");");
		sb.println("}else{");
		sb.println("MessageBox.show(r.readString());");
		sb.println("}");
		
		sb.println("});");		

		sb.println("}");
		sb.println();

		return sb.toString();
	}

	/**
	 * 
	 * <p>
	 * 获取方法参数名称
	 * </p>
	 * 
	 * @param cm
	 * @return
	 */
	protected static String[] getMethodParamNames(CtMethod cm) {
		MethodInfo methodInfo = cm.getMethodInfo();
		CodeAttribute codeAttribute = methodInfo.getCodeAttribute();
		LocalVariableAttribute attr = (LocalVariableAttribute) codeAttribute
				.getAttribute(LocalVariableAttribute.tag);

		String[] paramNames = null;
		try {
			paramNames = new String[cm.getParameterTypes().length];
		} catch (NotFoundException e) {
			e.printStackTrace();
		}
		int pos = Modifier.isStatic(cm.getModifiers()) ? 0 : 1;
		for (int i = 0; i < paramNames.length; i++) {
			paramNames[i] = attr.variableName(i + pos);
		}
		return paramNames;
	}

	/**
	 * 获取方法参数名称，按给定的参数类型匹配方法
	 * @param calzz
	 * @param method
	 * @return
	 */
	public static String[] getMethodParamNames(Class<?> calzz, Method method) {
		ClassPool pool = ClassPool.getDefault();
		CtClass cc = null;
		CtMethod cm = null;
		try {
			cc = pool.get(calzz.getName());

			String[] paramTypeNames = new String[method.getParameterTypes().length];
			for (int i = 0; i < method.getParameterTypes().length; i++)
				paramTypeNames[i] = method.getParameterTypes()[i].getName();

			cm = cc.getDeclaredMethod(method.getName(), pool.get(paramTypeNames));
		} catch (NotFoundException e) {
			e.printStackTrace();
		}
		return getMethodParamNames(cm);
	}
}
