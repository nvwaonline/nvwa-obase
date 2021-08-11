package online.nvwa.obase.tools;

import javassist.*;
import javassist.bytecode.CodeAttribute;
import javassist.bytecode.LocalVariableAttribute;
import javassist.bytecode.MethodInfo;
import online.nvwa.obase.tools.FuncScope.Scope;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Hashtable;

//import com.kbase.proxy.CommUser2;

public class Java2JS {
	public static Hashtable<String, String> types = new Hashtable<String, String>();
	static{
		types.put("boolean", "Boolean");
		types.put("byte",  "Byte");
		types.put("short", "Short");
		types.put("int",  "Integer");
		types.put("float", "Float");
		types.put("double", "Double");
		types.put("long", "Long");
		
		types.put("Boolean", "Boolean");
		types.put("Byte",  "Byte");
		types.put("Short", "Short");
		types.put("Integer", "Integer");
		types.put("Float", "Float");
		types.put("Double", "Double");
		types.put("Long", "Long");
		types.put("byte[]", "byte[]");
		types.put("String", "String");		
		types.put("String[]", "String[]");		
	}
	
//	public static void main(String[] args) throws IOException {
//		String source = class2as(CommUser2.class);
//		System.out.println(source);
//	}

	public static String class2js(Class<?> c){		
		StringBuffer sb = new StringBuffer();
		sb.append("function createRequest(){\n");
		sb.append("var r = {method:'',paras:new Array()};\n");
		sb.append("return r;\n");
		sb.append("}\n");

		sb.append("function createProxy(){\n");
		sb.append("var proxy = new Object();\n");

		Method[] ms = c.getMethods();

		for(Method m : ms){
            if(m.isAnnotationPresent(FuncScope.class)){
            	FuncScope funcScope = (FuncScope) m.getAnnotation(FuncScope.class);
            	if(funcScope.value() == Scope.APPLICATION){
        			sb.append(method2js(m.getDeclaringClass(), m));
            	}
            }
		}

		sb.append("return proxy;\n");
		sb.append("}\n\n");

		sb.append("var proxy = createProxy();\n");
		sb.append("export{proxy}\n");
		
		return sb.toString();		
	}

	@SuppressWarnings("unused")
	public static String method2js(Class<?> clazz, Method m){
		String type = m.getReturnType().getSimpleName();
//		System.out.println(type + "\t" + m.getName());
		boolean isVoid = type.equals("void");
		
		String[] paraNames = getMethodParamNames(clazz, m);
		PrintBuffer sb = new PrintBuffer();
		sb.print("proxy.");
		sb.print(m.getName());
		sb.print("=function(");
		int i = 0;
		for(Parameter p : m.getParameters()){
			if(i>0)sb.print(", ");
			sb.print(paraNames[i]);
			i++;
		}
		if(!isVoid){
			if(i>0)sb.print(", ");
			sb.print("callback");
		}
		sb.print(")");
		sb.println("{");


		sb.println("var p = createRequest();");
		sb.println("p.method = '" + m.getName()+"';");
	
		i = 0;
		for(Parameter p : m.getParameters()){
			sb.print("p.paras["+i+"]=");
//			sb.print("{type:'"+p.getType().getSimpleName()+"',value:''+");
			sb.print("{type:'"+ types.get(p.getType().getSimpleName()) +"',value:''+");
			if("String[]".equals(p.getType().getSimpleName()))
				sb.print("JSON.stringify("+paraNames[i]+")");
			else
				sb.print(paraNames[i]);
			
			sb.println("};");
			i++;
		}
		
		if(!isVoid){
			sb.println("this.server.sendRequest(p, function(r){");
			sb.println("if(callback)callback(r);");		
			sb.println("});");		
		}else{
			sb.println("this.server.sendRequest(p);");		
		}

		sb.println("};");
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
