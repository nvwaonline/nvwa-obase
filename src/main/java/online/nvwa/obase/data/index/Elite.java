package online.nvwa.obase.data.index;

import java.util.List;
import java.util.Vector;

public class Elite {
	private byte[] row;
	private List<Find> matchs = new Vector<Find>();
	private double score;

	public Elite(byte[] row){
		this.setRow(row) ;
	}

	public byte[] getRow() {
		return row;
	}

	public void setRow(byte[] row) {
		this.row = row;
	}
	
	public void addMatch(Find match){
		this.matchs.add(match);
	}
	
	public List<Find> getMatchs() {
		return matchs;
	}
	
	public void setScore(double value){
		this.score = value;
	}
	public double getScore(){
		return score;
	}
}
