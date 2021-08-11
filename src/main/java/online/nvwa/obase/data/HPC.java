package online.nvwa.obase.data;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import javax.script.ScriptException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;


/**
 * a.b.c.d = a; 
 * r = a.b;
 * a.b = r;
 *
 */
public class HPC{
	//脚本添加
	private StringBuffer sb = new StringBuffer();
	public HPC append(String sentence){
		sentence = sentence.replaceAll(" ", "");
		sb.append(sentence);
		if(!sentence.endsWith(";")){
			sb.append(";"); 
		}
		
		return this;
	}
	
	//输入与输出
	private HashMap<String, Object> inputs = new HashMap<String, Object>();
	public void put(String name, Object v){
		inputs.put(name, v);
	}
	public Object get(String name){
		return inputs.get(name);
	}
	public HPC clear(){
		return clearScript().clearVars();
	}
	public HPC clearScript(){
		sb = new StringBuffer();
		return this;
	}
	
	public HPC clearVars(){
		inputs.clear();
		return this;
	}
	
	public byte[] getBytes(String name) throws ScriptException{
		Object value = inputs.get(name);
		if(value instanceof Cell){
			return CellUtil.cloneValue((Cell)value);
		}
		
		if(value instanceof byte[])
			return (byte[])value;
		
		throw new ScriptException("var ["+ name +"] can't convert to byte[]");
	}
	public long getTimeStamp(String name) throws ScriptException{
		Object value = inputs.get(name);
		if(value instanceof Cell){
			return  ((Cell)value).getTimestamp();
		}
		
		throw new ScriptException("var ["+ name +"] not instance of Cell");
	}
	public Table getTable(String name) throws ScriptException{
		Object value = inputs.get(name);

		if(value instanceof Table)
			return (Table)value;
		
		throw new ScriptException("var ["+ name +"] can't convert to Table");
	}

	@SuppressWarnings("unchecked")
	public List<Cell> getList(String name) throws ScriptException{
		Object value = inputs.get(name);

		if(value instanceof List)
			return (List<Cell>)value;
		
		throw new ScriptException("var ["+ name +"] can't convert to List<Cell>");
	}

	public boolean exist(String name){
		return inputs.containsKey(name) && get(name)!=null;
	}
	public boolean isBytes(String name){
		return exist(name) && ((get(name) instanceof byte[]) || (get(name) instanceof Cell)) ;
	}
	public boolean isList(String name){
		return exist(name) && (get(name) instanceof List);
	}
	public boolean isTable(String name){
		return exist(name) && (get(name) instanceof Table);
	}
	
	
//	private static Hashtable<String, Table> tables = new Hashtable<String, Table>();
	
	public HPC set(String name, Table v){
		put(name, v);
		return this;
	}
	
	public HPC set(String name, byte[] v){
		put(name, v);
		return this;
	}
	public HPC set(String name){
		return set(name, name);
	}
	public HPC setList(String name){
		return set(name, new Vector<Cell>());
	}
	public HPC set(String name, String v){
		put(name, Bytes.toBytes(v));
		return this;
	}	
	public HPC set(String name, boolean v){
		put(name, Bytes.toBytes(v));
		return this;
		
	}	
	public HPC set(String name, short v){
		put(name, Bytes.toBytes(v));
		return this;
	}	
	public HPC set(String name, int v){
		put(name, Bytes.toBytes(v));
		return this;
	}	
	public HPC set(String name, long v){
		put(name, Bytes.toBytes(v));
		return this;
	}	
	public HPC set(String name, float v){
		put(name, Bytes.toBytes(v));
		return this;
	}	
	public HPC set(String name, double v){
		put(name, Bytes.toBytes(v));
		return this;
	}	
	public HPC set(String name, Result v){
		put(name, v.listCells());
		return this;
	}	
	public HPC set(String name, List<Cell> v){
		put(name, v);
		return this;
	}	
	public HPC set(String name, Put v) throws IOException{
		List<Cell> cells = new Vector<Cell>();
		CellScanner cs = v.cellScanner();
		while(cs.advance()){
			cells.add(cs.current());
		}		
		put(name, cells);
		
		return this;
	}	
	
	
	
//	public void put(String def, byte[] bytes){
//	}
//	
//	public Result get(String def){
//		return null;
//	}
	
	//1. 根据;进行语句分割
	public HPC exec() throws ScriptException{
		String script = sb.toString();
		String[] commands = script.split(";");
		for(int i=0; i<commands.length; i++){
			execIfCommand(commands[i]);
		}
		
		return this;
	}

	public HPC exec(String script) throws ScriptException{
		String[] commands = script.replaceAll(" ", "").split(";");
		for(int i=0; i<commands.length; i++){
			execIfCommand(commands[i]);
		}
		
		return this;
	}
	
	//2. 判断是否是判断执行a?b:c
	private void execIfCommand(String comm) throws ScriptException{
		if(comm == null || comm.length() ==0)
			return;
		
		int pos = comm.indexOf("?");
		if(pos < 0){
			execCommand(comm);
			return;
		}
		
		if(pos ==0)throw new ScriptException(comm);			
		
		String judge = comm.substring(0, pos);
		String content = comm.substring(pos+1);
		if(content == null)throw new ScriptException(comm);	
		
		String[] segs = content.split(":");
		switch(segs.length){
		case 0:
			return;
		case 1:
			if(notNull(judge))
				execCommand(segs[0]);
			return;
		case 2:
			if(notNull(judge))
				execCommand(segs[0]);
			else
				execCommand(segs[0]);
			return;
		default:
			throw new ScriptException(comm);
		}
	}
	
	//3. 根据=分成左右两个部分
	private void execCommand(String comm) throws ScriptException{
		if(comm == null)return;
		
		String[] segs = comm.split("=");
		if(segs.length != 2)throw new ScriptException(comm);
		
		assignment(segs[0], eval(segs[1]));
	}
	
	private boolean notNull(String exp) throws ScriptException{
		return eval(exp) != null;
	}
	
	private Object eval(String exp) throws ScriptException{
		Ana ana = this.analysis(exp);
		
		return eval(ana);
	} 
	
	private Ana analysis(String exp) throws ScriptException{
		//支持null赋值
		if(exp == null)throw new ScriptException("eval empty string");
		
		String[] segs = exp.split("#");
		String[] vars = null;
		Long ts = null;
		switch(segs.length){
		case 1:
			vars = segs[0].split("\\."); 
			break;
		case 2:
			vars = segs[0].split("\\.");
			ts = Bytes.toLong(getBytes(segs[1]));
			break;
		default:
			throw new ScriptException("eval empty string");
		}
		
		List<String> pures = new Vector<String>();
		for(String s : vars){
			if(s != null){
				pures.add(s);
			}
		}
		if(pures.size()>4){
			throw new ScriptException("too much segs: " + exp);
		}
		
		Ana ret = new Ana();
		ret.vars = pures.toArray(new String[0]);
		ret.ts = ts;
		
		if(isTable(vars[0])){
			ret.remote = true;
		}
		
		return ret;
	}
	
	private Object eval(Ana ana) throws ScriptException{
		if(ana.vars.length == 1 && ana.vars[0].equals("null"))
			return null;
		//add
//		if(ana.vars.length == 1 && get(ana.vars[0]) == null)
//			return null;
		//
		
		for(String var : ana.vars){
			if(!exist(var))
				throw new ScriptException("找不到变量: " + var);
		}
		
		if(ana.remote)
			return evalRemote(ana);
		else
			return evalLocal(ana);
	}
	
	//获取表变量的值
	private Object evalRemote(Ana ana) throws ScriptException{
		if(ana.vars.length<2 || ana.vars.length >4)
			throw new ScriptException("evalRemote: 参数个数不正确 " + ana.vars.length);
		for(int i=1; i<ana.vars.length; i++){
			if(!isBytes(ana.vars[i]))
    			throw new ScriptException("evalRemote: 数据库访问字段["+ana.vars[i]+"]不是byte[]类型");
		}
		Get get=new Get(getBytes(ana.vars[1]));
		switch(ana.vars.length){
		case 3: //table row cf
			get.addFamily(getBytes(ana.vars[2]));
			break;
		case 4: //table row cf column
			get.addColumn(getBytes(ana.vars[2]), getBytes(ana.vars[3]));
			break;
		}

		try {
			if(ana.ts != null)get.setTimeStamp(ana.ts);
			Result res = getTable(ana.vars[0]).get(get);
			
			List<Cell> raws = res.listCells();
			if(raws == null)return null;
			
			if(ana.vars.length == 4){
				if(raws.size() == 0)
					return null;
				if(raws.size() >1)
        			throw new ScriptException("evalRemote: 数据库访问单列数据超过1个");
    			return raws.get(0);  				
			}
			return raws;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ScriptException(e.toString());
		}
	}
	
	//获取本地变量的值
	private Object evalLocal(Ana ana) throws ScriptException{
		if(ana.vars.length == 0)
			return null;

		if(ana.vars.length == 1)
			return get(ana.vars[0]);
		
		//第一个变量必须是容器类型；		
		if(!isList(ana.vars[0]))
			throw new ScriptException("表达式["+ana.exp+"]中的变量"+ana.vars[0]+"不是一个List对象");
		//其他的必须是值类型；		
		for(int i=1; i<ana.vars.length; i++){
			if(!isBytes(ana.vars[i]))
				throw new ScriptException("表达式["+ana.exp+"]中的变量"+ana.vars[i]+"不是一个byte[]对象");
		}

		switch(ana.vars.length){
		case 2: //v, row
			return CUtil.get(getList(ana.vars[0]), getBytes(ana.vars[1]));
		case 3: //v, row, family
			return CUtil.get(getList(ana.vars[0]), getBytes(ana.vars[1]), getBytes(ana.vars[2]));
		case 4: //v, row, family, column
			return CUtil.get(getList(ana.vars[0]), getBytes(ana.vars[1]), getBytes(ana.vars[2]), getBytes(ana.vars[3]));
		}
		
		throw new ScriptException("表达式["+ana.exp+"]无法计算");
	}

	private void assignment(String exp, Object value) throws ScriptException{
		Ana ana = this.analysis(exp);		
//		if(ana.ts != null && !ana.remote)
//			throw new ScriptException("容器赋值语句等式左边不能有版本信息["+ana.exp+"]");
		
		if(ana.remote)
			assignmentRemote(ana, value);
		else
			assignmentLocal(ana, value);
	}
	
	/**
	 * 远程变量赋值
	 * @param ana
	 * @param value
	 * @throws ScriptException
	 */
	@SuppressWarnings("unchecked")
	private void assignmentRemote(Ana ana, Object value) throws ScriptException{
		if(ana.vars.length<2 || ana.vars.length >4)
			throw new ScriptException("assiRemote: 参数个数不正确 " + ana.vars.length);
		for(int i=1; i<ana.vars.length; i++){
			if(! isBytes(ana.vars[i]))
    			throw new ScriptException("assiRemote: 数据库访问字段["+ana.vars[i]+"]不是byte[]类型");
		}

		Put put=new Put(getBytes(ana.vars[1]));
		switch(ana.vars.length){
		case 2: //write row
			fillPut(put, (List<Cell>)value, ana.ts);
			break;
		case 3: //table row cf
			fillPut(put, getBytes(ana.vars[2]), (List<Cell>)value, ana.ts);
			break;
		case 4: //table row cf column
			fillPut(put, getBytes(ana.vars[2]), getBytes(ana.vars[3]), (byte[])value, ana.ts);
			break;
		}

		try {
//			put.setDurability(Durability.SKIP_WAL);
			getTable(ana.vars[0]).put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ScriptException(e.toString());
		}
	}
	
	//给row赋值
	private void fillPut(Put put, List<Cell> value, Long ts){
		for(Cell c : value){
			if(ts == null)
				put.addColumn(CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c), CellUtil.cloneValue(c));
			else
				put.addColumn(CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c), ts, CellUtil.cloneValue(c));
		}		
	}
	//给family赋值
	private void fillPut(Put put, byte[] family, List<Cell> value, Long ts){
		for(Cell c : value){
			if(ts == null)
				put.addColumn(family, CellUtil.cloneQualifier(c), CellUtil.cloneValue(c));
			else
				put.addColumn(family, CellUtil.cloneQualifier(c), ts, CellUtil.cloneValue(c));
		}		
	}
	//给column赋值
	private void fillPut(Put put, byte[] family, byte[] column, byte[] value, Long ts){
		if(ts == null || ts<0)
			put.addColumn(family, column, value);
		else
			put.addColumn(family, column, ts, value);
	}
	
	/**
	 * 本地变量赋值
	 * @param ana
	 * @param value
	 * @throws ScriptException
	 */
	private void assignmentLocal(Ana ana, Object value) throws ScriptException{
		//如果只有一个变量，直接赋值
		if(ana.vars.length == 1){
			put(ana.vars[0], value);
			return;
		}
		
		//有两个及以上字段的为对容器操作，不管哪一种，
		for(int i=1; i<ana.vars.length; i++){
			if(! isBytes(ana.vars[i]))
    			throw new ScriptException("assiLocal: 目标字段["+ana.vars[i]+"]不是byte[]类型");
		}
		
		//两个以上的变量，必须是容器赋值
		//如果不存在,或者为byte[]
		if(!exist(ana.vars[0]) || isBytes(ana.vars[0])){
			setList(ana.vars[0]);
		}
		
		if(ana.vars.length<2 || ana.vars.length >4)
			throw new ScriptException("assiRemote: 参数个数不正确 " + ana.vars.length);
		
		List<Cell> container = getList(ana.vars[0]);

		switch(ana.vars.length){
		case 2: //write row    value必须是List或者Cell类型  整行替换
			CUtil.assiRow(container, getBytes(ana.vars[1]), value, ana.ts);
			break;
		case 3: //table row cf  value必须是List或者Cell类型  整family替换
			CUtil.assiFamily(container, getBytes(ana.vars[1]), getBytes(ana.vars[2]), value, ana.ts);
			break;
		case 4: //table row cf column  value必须是byte[]或者Cell类型 整列替换
			CUtil.assiColumn(container, getBytes(ana.vars[1]), getBytes(ana.vars[2]), getBytes(ana.vars[3]), value, ana.ts);
			break;
		}
	}

	
	private class Ana {
		String exp;
    	boolean remote = false;
    	String[] vars;
    	Long ts;    	
    } 
}
