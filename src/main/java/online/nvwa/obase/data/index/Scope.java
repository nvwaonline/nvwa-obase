package online.nvwa.obase.data.index;

import online.nvwa.obase.data.Bytez;
import online.nvwa.obase.utils.Obase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

public class Scope {
	public Idc idc;
	public Value start;
	public Value stop;

	public Scope(Idc idc, Value start, Value stop){
		this.idc = idc;
		this.start = start;
		this.stop = stop;
	}

	/**
	 * 获取标签相关索引行
	 * @return 索引行列表
	 */
	public List<Find> getIndexRows(){
		Scan scan = new Scan(Bytez.add(Bytez.from(idc.getId()), start.ranking()), Bytez.add(Bytez.from(idc.getId()), stop.ranking()));

		ResultScanner rs = null;
		try {
			rs = Obase.getIndexTable().getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
			return new Vector<Find>();
		}

		List<Find> rets = new Vector<Find>();

		int type = this.start.getType();

		for(Result r: rs){
			int len = 4+this.start.getData().length;
			
			Cell c = r.getColumnLatestCell(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("data"));
			if(c != null){
				switch(type){
				//固定长度
				case Value.Type_Boolean:
					len += 0;
					break;
					//固定符号
				case Value.Type_Byte:
				case Value.Type_Short:
				case Value.Type_Int:
				case Value.Type_Long:
				case Value.Type_Float:
				case Value.Type_Double:
					len += 1;
					break;

					//可变长度
				case Value.Type_String:
				case Value.Type_Bytes:
					len = 4 + 2 + Bytez.toShort(r.getRow(),4);
					break;
				}

				byte[] row = Bytes.copy(r.getRow(),len,r.getRow().length-len);
				byte[] data = Index.fromData(CellUtil.cloneValue(c));
				rets.add(new Find(null, row, data));
			}
		}

		return rets;

	}	

	
	/**
	 * 计算给定的行集合和标签的行集合的交集
	 * @param rows
	 * @return
	 */
	public List<Find> filterIndexRows(List<Find> rows){
		if(idc == null || idc.getRefColumn() == null || idc.getRefTable() == null)
			return new Vector<Find>();
		
		//make get
		List<Get> gets = new Vector<Get>();

		for(Find row : rows){
			Get get = new Get(row.getRow());
			get.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from(idc.getRefColumn()));
			gets.add(get);
		}

		Result[] results = null;
		try {
			results = idc.getRefTable().get(gets);
		} catch (IOException e) {
			e.printStackTrace();
			return new Vector<Find>();
		}
		List<Find> rets = new Vector<Find>();

		for(Result r: results){
			Cell c = r.getColumnLatestCell(Bytez.from(Obase.FAMILY_ATTR), Bytez.from(idc.getRefColumn()));
			if(c != null){
				byte[] value= CellUtil.cloneValue(c);
				if(Value.inScope(start, stop, value)){
					rets.add(new Find(null, r.getRow(), null));
				}
			}
		}

		return rets;
	}	
	
	
	/**
	 * 计算给定的行集合和标签的行集合的交集
	 * @param rows
	 * @return
	 */
	public List<Find> filterIndexRows_byScan(List<Find> rows){
		Scan scan = new Scan(Bytez.add(Bytez.from(idc.getId()), start.ranking()), Bytez.add(Bytez.from(idc.getId()), stop.ranking()));

		ResultScanner rs = null;
		try {
			rs = Obase.getIndexTable().getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
			return new Vector<Find>();
		}

		List<Find> rets = new Vector<Find>();
		Set<byte[]> filters = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		for(Find f : rows){
			filters.add(f.getRow());
		}


		int type = this.start.getType();
	
		for(Result r: rs){
			int len = 4+this.start.getData().length;
			
			Cell c = r.getColumnLatestCell(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("data"));
			if(c != null){
				switch(type){
				//固定长度
				case Value.Type_Boolean:
					len += 0;
					break;
					//固定符号
				case Value.Type_Byte:
				case Value.Type_Short:
				case Value.Type_Int:
				case Value.Type_Long:
				case Value.Type_Float:
				case Value.Type_Double:
					len += 1;
					break;

					//可变长度
				case Value.Type_String:
				case Value.Type_Bytes:
					len = 4 + 2 + Bytez.toShort(r.getRow(),4);
					break;
				}

				byte[] row = Bytes.copy(r.getRow(),len,r.getRow().length-len);
				byte[] data = Index.fromData(CellUtil.cloneValue(c));
				if(filters.contains(row)){
					rets.add(new Find(null, row, data));
				}
			}
		}

		return rets;
	}	
}
