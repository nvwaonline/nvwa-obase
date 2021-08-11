package online.nvwa.obase.data;

import javax.script.ScriptException;
import java.io.IOException;

public class Test {
	private static void test(){ 
		String table = "t_docs";
		String row = "row1";
		String family = "family";
		String column = "column";
		long time = System.currentTimeMillis();

		HPC hpu = new HPC();
		
		//设置输入变量
		hpu.set("t", table);
		hpu.set("r", row);
		hpu.set("f", family);
		hpu.set("c", column);
		hpu.set("ts", time);
		
		//添加脚本并执行
		hpu.append("v = t.r.f.c:t");
		hpu.append("v = t.r.f.c#t");
		hpu.append("v = ts");
		hpu.append("t.r.f.c#t = t");
		hpu.append("h?:u=a");
		
		//执行脚本
		try {
			hpu.exec();
		} catch (ScriptException e) {
			e.printStackTrace();
		}
		
		//返回结果
		hpu.get("u");
	}

	public static void main(String[] args) throws IOException {
		String[] segs = "::a".split(".");
		System.out.println("segs = " +segs.length);
		
		test();

	}

}
