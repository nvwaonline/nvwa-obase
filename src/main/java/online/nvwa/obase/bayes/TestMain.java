package online.nvwa.obase.bayes;

public class TestMain {
    public static void main(String[] args) throws Exception {
    	String line ="Hello, Sandy. What is some childrens, gagaga It is early,  I got it. happiness happy done dead liked running consolingly";
    	line = line.toLowerCase();
    	String res[] = line.split("[^a-zA-Z]");
    	
    	int i = 0;
    	for(String seg : res){
    		if(seg != null && seg.length()>0){
    	    	Porter s = new Porter(); 
    	    	s.add(seg.toCharArray() , seg.length());
    	    	s.stem();
     		
    			System.out.println(i++ + ":" +s.toString() );
    		}
    	}
    }  
}
