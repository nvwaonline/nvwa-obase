package online.nvwa.obase.data.table;

import online.nvwa.obase.data.Bytez;
import online.nvwa.obase.data.Entity;
import online.nvwa.obase.data.RemoteObject;
import online.nvwa.obase.utils.Obase;
import online.nvwa.obase.utils.Util;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class TFilePath extends Entity {
	@SuppressWarnings("unused")
	private final static String tableName = "obase:storagepath";
	
	public TFilePath(String path) {
		super(Bytez.from(path));
	}
	
	public String getPath(){
		return Bytez.toString(this.getRow());
	}
	
	//如果还没有分配id,分配id
	@Override
	public RemoteObject connect(){
		if(this.needConnect()){
			if(this.getId() == 0){
				this.setId(Util.getDiscreteID(this.getBasisTable(), "filpath_id"));
			}
			
			super.connect();
		}		
		return this;
	}
	
	@Override
	public Table getBasisTable() {
		// TODO Auto-generated method stub
		return Obase.getFilePathTable();
	}
	
	//path id
	private void setId(long id){
		this.setAttribute(Obase.COLUMN_ID, Bytez.from(id));
	}
	public long getId(){
		byte[] bytes = this.getAttribute(Obase.COLUMN_ID);
		if(bytes == null)
			return 0;

		return Bytes.toLong(bytes);
	}
}
