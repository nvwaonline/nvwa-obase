package online.nvwa.obase.tools;

public class PrintBuffer{
	private StringBuffer sb = new StringBuffer();
	public void print(String str){
		sb.append(str);
	}
	
	public void println(String str){
		sb.append(str);
		sb.append("\n");
	}

	public void println(){
		sb.append("\n");
	}
	
	public String toString(){
		return sb.toString();
	}

}
