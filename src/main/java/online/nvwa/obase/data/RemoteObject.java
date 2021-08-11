package online.nvwa.obase.data;

public abstract class RemoteObject {
//	public abstract Table getBasisTable();
	public abstract byte[] getRow();
	public abstract boolean needConnect();
	public abstract RemoteObject connect();
	public abstract byte[] get(byte[] column);
	public abstract void set(byte[] column, byte[] data);
}
