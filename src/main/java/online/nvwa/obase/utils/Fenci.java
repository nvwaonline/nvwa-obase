package online.nvwa.obase.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;

public class Fenci {
    /**
     * tika文档内容抽取
     * @param is
     * @return
     */
    public static String tikaText(InputStream is){
    	try{
//    		AutoDetectParser parser = new AutoDetectParser();
//    		BodyContentHandler handler = new BodyContentHandler(1024*1024*1024);
//    		Metadata metadata = new Metadata();
//    		parser.parse(is, handler, metadata);
    		
//			System.out.println("Author: " + metadata.get("Author"));
//    		
//    		String[] metadataNames = metadata.names();
//
//    		for(String name : metadataNames) {		        
//    			System.out.println(name + ": " + metadata.get(name));
//    		}
    	      
//    		return handler.toString();
    		return null;
    	}catch(Exception e){
    		return null;
    	}
    }   
    
 
    public static Hashtable<String, Integer> text2words(String text) throws IOException{
    	Hashtable<String, Integer> words = new Hashtable<String, Integer>();
    	if(text==null||text=="")return words;

//    	Segment shortestSegment = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
//    	for (Term t: shortestSegment.seg(text))
//    	{
//    		String word = t.word;
//    		Integer value = words.getOrDefault(word, 0);
//    		words.put(word, value+1);
//    	}
    	return words;
    }    

   
}
