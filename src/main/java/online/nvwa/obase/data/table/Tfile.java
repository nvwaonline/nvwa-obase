package online.nvwa.obase.data.table;

import online.nvwa.obase.data.TimeEntity;
import online.nvwa.obase.utils.Obase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

public class Tfile extends TimeEntity{
	@SuppressWarnings("unused")
	private final static String tableName = "obase:storage";
	
	public Tfile(byte[] row){
		super(row);
	}
	
	public Tfile(Result r){
		super(r);
	}

	@Override
	public Table getBasisTable() {
		// TODO Auto-generated method stub
		return Obase.getFileTable();
	}
}
