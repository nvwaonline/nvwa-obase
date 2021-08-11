package online.nvwa.obase.data.index;

import online.nvwa.obase.data.Bytez;
import online.nvwa.obase.data.Entity;
import online.nvwa.obase.data.RemoteObject;
import online.nvwa.obase.utils.LruMap;
import online.nvwa.obase.utils.Obase;
import online.nvwa.obase.utils.Util;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

/**
 * Idc - Index Table Column
 * 用于索引的表列，根据表名与列明查找对应的ID
 * @author HF-36
 */
public class Idc extends Entity{
	@SuppressWarnings("unused")
	private final static String tableName = "obase:idc";
	
	private static LruMap<String, Idc> idcCache = new LruMap<String, Idc>(10000);
	private Table refTable;//
	private String refColumn;

	/**
	 * 此处最好能够有全局同步，生成Idc.  如果多个JVM并发访问，同时创建可能会造成多个实例、同一意义的Idc有多个不同的id的问题。
	 * @param table
	 * @param column
	 * @return
	 */
	synchronized public static Idc getInstance(Table table, String column){
		String key = combineTag(table, column);
		Idc idc = idcCache.get(key);
		if(idc == null) {
			idc = new Idc(table, column);
			idcCache.put(key, idc);
			idc.connect();
		}
		return idc;
	}

	private Idc(Table table, String column) {
		this(Bytez.from(combineTag(table, column)));
		this.refTable = table;
		this.refColumn = column;
	}
	public Idc(byte[] row) {
		super(row);
	}
	public Idc(Result r) {
		super(r);
	}
	
	public Table getRefTable(){
		return refTable;
	}
	
	public String getRefColumn(){
		return refColumn;
	}
	
	//如果还没有分配id,给tag分配id
	@Override
	public RemoteObject connect(){
		if(this.needConnect()){
			if(this.getId() == 0){
				this.setId(Util.getDiscreteIntID(this.getBasisTable(), "tagUnifyIdentify"));
			}
			
			super.connect();
		}		
		
		return this;
	}
	
	public static String combineTag(Table table, String column){
		return table.getName().getNameWithNamespaceInclAsString()+"|" + column;
	}
	
	/*
	 * 得到一个列对象衍生的值对象列表。
	 * startRow 扫描的起始行，为空则忽略 
	 * maxResult 控制每次返回的结果数量，防止一次返还过多
	 * 
	 * 注意：可能要考虑是否要把 起始地址排除在外
	 */
	public List<Idx> getIdxs(byte[] startRow, int maxResult){
		Scan scan = new Scan(Bytez.from(this.getId()), Bytez.from(this.getId()+1));
		if(startRow != null){
			scan.setStartRow(startRow);
		}		
		
		try {
			ResultScanner rs = Obase.getIdxTable().getScanner(scan);

			List<Idx> rows = new Vector<Idx>();
			for(Result r: rs){
				rows.add(new Idx(r));
				if(rows.size() >= maxResult )
					break;
			}

			return rows;
		} catch (IOException e) {
			e.printStackTrace();
			return new Vector<Idx>();
		}
	}
	
	/**
	 * 得到一个表所有的使用索引的列对象
	 * @param table， 表对象
	 * @param startRow 扫描的起始行，为空则忽略 
	 * @param maxResult 控制每次返回的结果数量，防止一次返还过多
	 * @return
	 */
	public static List<Idc> getTableIdcs(Table table, byte[] startRow, int maxResult){
		String front = table.getName().getNameWithNamespaceInclAsString()+"|";
		Scan scan = new Scan(Bytes.toBytes(front), Bytez.next(Bytes.toBytes(front)));
//		Filter pf = new PrefixFilter(Bytes.toBytes(front));
//		scan.setFilter(pf);
		if(startRow != null){
			scan.setStartRow(startRow);
		}		
		
		try {
			ResultScanner rs = Obase.getIdcTable().getScanner(scan);

			List<Idc> rows = new Vector<Idc>();
			
			for(Result r: rs){
				rows.add(new Idc(r));
				if(rows.size() >= maxResult )
					break;
			}

			return rows;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new Vector<Idc>();
		}
	}

	@Override
	public Table getBasisTable() {
		return Obase.getIdcTable();
	}
	
	private void setId(int id){
		this.setAttribute(Obase.COLUMN_ID, Bytez.from(id));
	}
	public int getId(){
		byte[] bytes = this.getAttribute(Obase.COLUMN_ID);
		if(bytes == null)
			return 0;

		return Bytes.toInt(bytes);
	}
	
	public String getName(){
		return Bytez.toString(this.getRow());		
	}
	
	public void deleteIndexes() throws Exception{
		System.out.println("Delete index beginning.....");
		Scan scan = new Scan(Bytez.from(this.getId()), Bytez.from(this.getId() + 1));
		ResultScanner rs = Obase.getIndexTable().getScanner(scan);

		int count = 0;
		List<Delete> deletes = new Vector<Delete>();
		for(Result r: rs){
			deletes.add(new Delete(r.getRow()));
			if(deletes.size() >= 10000){
				Obase.getIndexTable().delete(deletes);
				deletes.clear();
				count++;
				System.out.println("Delete index: " + count*10000);
			}
		}
		
		Obase.getIndexTable().delete(deletes);		
		System.out.println("Delete index ended");
	}
	
	public void deleteIdxes() throws Exception{
		System.out.println("Delete idx beginning.....");
		Scan scan = new Scan(Bytez.from(this.getId()), Bytez.from(this.getId() + 1));
		ResultScanner rs = Obase.getIdxTable().getScanner(scan);

		int count = 0;
		List<Delete> deletes = new Vector<Delete>();
		for(Result r: rs){
			deletes.add(new Delete(r.getRow()));
			if(deletes.size() >= 10000){
				Obase.getIdxTable().delete(deletes);
				deletes.clear();
				count++;
				System.out.println("Delete idx: " + count*10000);
			}
		}
		
		Obase.getIdxTable().delete(deletes);		
		System.out.println("Delete idx ended");
	}
}
