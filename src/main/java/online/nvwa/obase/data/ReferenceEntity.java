package online.nvwa.obase.data;

import online.nvwa.obase.utils.Obase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * 可以记录引用对象数量的数据库实体
 */
public abstract class ReferenceEntity extends TimeEntity{
	public ReferenceEntity(byte[] row) {
		super(row);
	}	
	
	public ReferenceEntity(Result r) {
		super(r);
	}
	
	/**
	 * 关联次数
	 * @return
	 */
	public long getReference(){
		byte[] bytes = this.getAttribute(Obase.COLUMN_REFERENCE);
		if(bytes == null)
			return 0;

		return Bytez.toLong(bytes);
	}
	
	public void setReference(long value){
		this.setAttribute(Obase.COLUMN_REFERENCE, Bytez.toBytes(value));
	}	
	
	public long incReference(int num){
		long ref = this.getReference()+num;
		this.setReference(ref);
		return ref;
	}
	
	public long decReference(){
		long ref = this.getReference()-1;
		this.setReference(ref);
		return ref;
	}	
	
	public long increment(String counter, int count) {
		try {
			Increment inc = new Increment(this.getRow());
			inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes(Obase.COLUMN_REFERENCE) ,1);
			Result res = this.getBasisTable().increment(inc);
			List<Cell> cells = res.listCells();
			return Bytes.toLong(CellUtil.cloneValue(cells.get(0)));
		}catch(Exception e) {
			return 0;
		}
	}
}
