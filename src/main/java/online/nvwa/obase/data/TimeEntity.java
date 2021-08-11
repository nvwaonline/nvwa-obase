package online.nvwa.obase.data;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

/**
 * 文档类对象
 * @author Victor.Shaw
 * 具有创建时间、词语总数、被引用数次数属性，
 */
public abstract class TimeEntity extends Entity {
	public TimeEntity(byte[] row) {
		super(row);
		
		//新建的对象增加createTime属性
		if(needConnect()){
			this.setAttribute("createTime", Bytes.toBytes(System.currentTimeMillis()));
		}
	}
	
	public TimeEntity(byte[] row, boolean autoload) {
		super(row, autoload);
		
		//新建的对象增加createTime属性
		if(needConnect()){
			this.setAttribute("createTime", Bytes.toBytes(System.currentTimeMillis()));
		}
	}

	public TimeEntity(Result r) {
		super(r);
	}
	
	public long getCreateTime(){
		return this.getAttributeAsLong("createTime", 0);
	}

	public Date getCreateDate(){
		return this.getAttributeAsDate("createTime");
	}
}
