package online.nvwa.obase.data;

public class Tag implements Comparable<Tag> {
	public String word;
	public double weight;
	
	public Tag(){
		this("", 0);
	}
	public Tag(String word, double weight){
		this.word = word ;
		this.weight = weight;
	}
	
	@Override
	public int compareTo(Tag arg0) {
		if(this ==arg0)return 0;		
		
		if(this.weight > arg0.weight)
			return -1;
		if(this.weight < arg0.weight)
			return 1;
		return this.word.compareTo(arg0.word);
	}
}
