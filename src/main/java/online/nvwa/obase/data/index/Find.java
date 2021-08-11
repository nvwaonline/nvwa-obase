package online.nvwa.obase.data.index;

public class Find {
	private Idx idx;
	private byte[] row;
	private byte[] data;
	
	public Find(Idx idx, byte[] row, byte[] data){
		this.idx = idx;
		this.row = row ;
		this.data = data;
	}

	public byte[] getRow() {
		return row;
	}

	public void setRow(byte[] row) {
		this.row = row;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public Idx getIdx() {
		return idx;
	}

	public void setIdx(Idx idx) {
		this.idx = idx;
	}

}
