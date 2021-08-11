package online.nvwa.obase.data.index;

import online.nvwa.obase.data.Bytez;
import online.nvwa.obase.data.Entity;
import online.nvwa.obase.utils.Obase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.util.stream.Stream;

public class Index extends Entity{
	@SuppressWarnings("unused")
	private final static String tableName = "obase:index";
	
	public Index(byte[] row) {
		super(row);
	}
	public Index(Result r) {
		super(r);
	}
	
	public Index(Idc idc, Value value, byte[] row){
		this(Bytez.add(Bytez.from(idc.getId()), value.ranking(), row));
	}

	@Override
	public Table getBasisTable() {
		return Obase.getIndexTable();
	}
	
	/**
	 *byte[] data;
	 */
	public void setData(byte[] value){
		this.setAttribute("data", toData(value));
	}
	public byte[] getData(){
		byte[] bytes = this.getAttribute("data");
		
		return fromData(bytes);
	}
	
	public static byte[] toData(byte[] value){
		if(value == null){
			value = Bytez.from(false);
		}else{
			value = Bytez.add(Bytez.from(true),value);
		}
		return value;
	}

	public static byte[] fromData(byte[] bytes){
		if(bytes==null || bytes.length==1)
			return null;
		
		return Bytez.copy(bytes, 1, bytes.length-1);
	}

	public static Stream<Index> stream(Scan scan){
		return Entity.stream(Index.class, scan).map(e->new Index(e));
	}
}
