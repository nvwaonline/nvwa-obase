package online.nvwa.obase.utils;

import online.nvwa.obase.data.Tag;

import java.util.Hashtable;
import java.util.List;

public class VectorUtil {
	public static double relation(double[] v1, double[] v2){
		if(v1.length != v2.length)
			return 0;
		
		return dotProduct(v1, v2)/(sizeOf(v1)*sizeOf(v2));
	}
	
	public static double dotProduct(double[] v1, double[] v2)
	{
		double ret = 0;
		for(int i=0; i<v1.length; i++){
			ret += v1[i]*v2[i];
		}
		
		return ret;		
	}
	
	//计算向量大小
	public static double sizeOf(double[] v)
	{
		double ret = 0;
		for(int i=0; i<v.length; i++){
			ret += v[i]*v[i];
		}
		
		return Math.sqrt(ret);		
	}

	public static double[] closeVector(double[] from, double[] to, double rate){
		double[] ret = new double[from.length];
		for(int i=0; i<ret.length; i++){
			ret[i] = from[i] + (to[i]-from[i])*rate;
		}
		
		return ret;
	}
	
	/**
	 * 计算标签向量之间的相似性
	 * @param a
	 * @param b
	 * @return
	 */	
	public static double calcSimilar(List<Tag> a, List<Tag> b){
		double length_a = 0;
		double length_b = 0;
		double product = 0;
		
		Hashtable<String, Double> words = new Hashtable<String, Double>();
		for(Tag t:a){
			length_a += t.weight*t.weight;
			words.put(t.word, t.weight);
		}
		for(Tag t:b){
			length_b += t.weight*t.weight;
			product += t.weight*words.getOrDefault(t.word, 0.0);
		}
		
		if(length_a == 0 || length_b == 0||product == 0)
			return 0;
		
//		a.forEach(t->words.put(t.word, t.weight));
//		double la = a.stream().map(t->t.weight*t.weight).reduce(0.0, (x,y)->x+y);
//		double lb = b.stream().map(t->t.weight*t.weight).reduce(0.0, (x,y)->x+y);
//		double lc = b.stream()
//				.filter(t->words.containsKey(t.word))
//				.map(t->t.weight*words.get(t.word))
//				.reduce(0.0, (x,y)->x+y);
		
		return product/Math.sqrt(length_a*length_b);
	}
}
