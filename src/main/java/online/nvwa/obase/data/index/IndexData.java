package online.nvwa.obase.data.index;

public class IndexData {
	private byte[] row;
	private byte[] attachment;

	public IndexData(byte[] row, byte[] attachment){
		this.row = row ;
		this.attachment = attachment;
	}

	public byte[] getRow() {
		return row;
	}
	public byte[] getAttachment(){return this.attachment;}
}
