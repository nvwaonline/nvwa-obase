package online.nvwa.obase.data.index;

import online.nvwa.obase.data.Bytez;
import online.nvwa.obase.data.ReferenceEntity;
import online.nvwa.obase.utils.Obase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 主要用于统计索引值出现的次数
 * @author Victor.Shaw
 */
public class Idx extends ReferenceEntity implements Comparable<Idx>{
	@SuppressWarnings("unused")
	private final static String tableName = "obase:idx";
	
	public Idx(byte[] row) {
		super(row);
	}

	public Idx(Result r) {
		super(r);
	}
	
	public Idx(Idc idc, Value value){
		this(Bytez.add(Bytez.from(idc.getId()), value.ranking()));
	}
	
	@Override
	public Table getBasisTable() {
		return Obase.getIdxTable();
	}
	
	@Override
	public int compareTo(Idx arg0) {
		if(this ==arg0)return 0;
			
		long v = this.getReference() - arg0.getReference();
		if(v > 0)return 1;
		if(v < 0)return -1;
		return 0;
	} 
	
	public String getName(){
		return Bytez.toString(this.getRow());		
	}
	
	/**
	 * 获取标签相关索引行
	 * @return 索引行列表
	 */
	public Stream<IndexData> stream(){
		byte[] irow = this.getRow();		
		Scan scan = new Scan().withStartRow(irow).withStopRow(Bytez.next(irow));

//        byte[] row = Bytes.copy(r.getRow(),irow.length,r.getRow().length-irow.length);

		return Index.stream(scan).map(e->new IndexData(
                Bytes.copy(e.getRow(),irow.length,e.getRow().length-irow.length),
		        e.getData()));
	}
	
	/**
	 * 获取标签相关索引行
	 * @return 索引行列表
	 */
	public List<Find> getIndexRows(){
		byte[] irow = this.getRow();
		
		Scan scan = new Scan(irow, Bytez.next(irow));
		
		ResultScanner rs = null;
		try {
			rs = Obase.getIndexTable().getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
			return new Vector<Find>();
		}
		List<Find> rows = new Vector<Find>();

		for(Result r: rs){
			Cell c = r.getColumnLatestCell(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("data"));
			if(c != null){
				byte[] row = Bytes.copy(r.getRow(),irow.length,r.getRow().length-irow.length);
				byte[] data = Index.fromData(CellUtil.cloneValue(c));
				rows.add(new Find(this, row, data));
			}
		}

		return rows;
	}
	
	/**
	 * 计算给定的行集合和标签的行集合的交集
	 * @param rows
	 * @return
	 */
	public List<Find> filterIndexRows(List<Find> rows){
		byte[] irow = this.getRow();
		
		//make get
		List<Get> gets = rows.stream()
				.map(t->new Get(Bytez.add(this.getRow(), t.getRow())))
				.collect(Collectors.toList());
		
		Result[] results = null;
		try {
			results = Obase.getIndexTable().get(gets);
		} catch (IOException e) {
			e.printStackTrace();
			return new Vector<Find>();
		}

		List<Find> rets = new Vector<Find>();
		for(Result r: results){
			Cell c = r.getColumnLatestCell(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("data"));
			if(c != null){
				byte[] row = Bytez.copy(r.getRow(),irow.length);
				byte[] data = Index.fromData(CellUtil.cloneValue(c));
				rets.add(new Find(this, row, data));
			}
		}

		return rets;
	}	
}
