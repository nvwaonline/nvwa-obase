package online.nvwa.obase.data;

import online.nvwa.obase.utils.Annealing;
import online.nvwa.obase.utils.VectorUtil;
import org.apache.hadoop.hbase.client.Result;

import java.util.*;
import java.util.stream.Collectors;

public abstract class RecommendEntity extends SearchableEntity{
	public RecommendEntity(byte[] row){
		super(row);
	}

	public RecommendEntity(Result r) {
		super(r);
	}
	
	/**
	 * 获取模拟退火系数24小时为周期
	 * @return
	 */
	public double getAneCoef(){return Annealing.FIRM_COEF;}
	/**
	 * 获取标签数
	 * @return
	 */
	public int getTagDimension(){return 100;}

	/**
	 * 添加标签，设置标签，获取标签，标签就是维度，利用标签之间的相似性计算对象的相似性  
	 */	
	public void learningTag(String tag, Double rate){
		byte[] data = this.getAttribute("tag", tag);
		double old = 0;
		if(data != null){
			old = Bytez.toDouble(data);
		}
		
		this.setAttribute("tag", tag, Bytez.from(Annealing.add(old, rate, this.getAneCoef())));
		//检查是否超出最大标签维度
		checkTagDimension();
	}
	
	public void learningTags_Old(List<Tag> tags, Double rate){
		if(tags == null || tags.isEmpty() || rate ==0)
			return;
		
		int len = tags.size();
		String[] items = new String[len];
		byte[][] values = new byte[len][];

		for(int i=0; i<len; i++){
			Tag tag = tags.get(i);
			byte[] data = this.getAttribute("tag", tag.word);
			double old = 0;
			if(data != null){
				old = Bytez.toDouble(data);
			}
			
			double learning = tag.weight*rate;

			items[i] = tag.word;
			values[i] = Bytez.from(Annealing.add(old, learning, this.getAneCoef()));
		}

		this.setAttribute("tag", items, values);
		//检查是否超出最大标签维度
		checkTagDimension();
	}	
	
	public void learning(RecommendEntity other, double rate){
		learningTags(other.getNormalTags(), rate);
	}
	
	public void learningTags(List<Tag> tags, Double rate){
		if(tags == null || tags.isEmpty() || rate ==0)
			return;
		List<Tag> oldTags = this.getTags();
		
		Map<String, Double> olds = new HashMap<String, Double>();
		Map<String, Double> curs = new HashMap<String, Double>();
		for(Tag t : oldTags){
			olds.put(t.word, t.weight);
		}
		curs.putAll(olds);

		for(Tag t:tags){
			double old = curs.getOrDefault(t.word, new Double(0));
			double learning = t.weight*rate;
			curs.put(t.word, Annealing.add(old, learning, this.getAneCoef()));
		}		
		List<Tag> curTags = new Vector<Tag>();
		for(String word : curs.keySet()){
			curTags.add(new Tag(word, curs.get(word)));
		}
		formatTags(curTags);//排序截取
		curs.clear();
		for(Tag t:curTags){
			curs.put(t.word, t.weight);
		}

		//update
		List<String> updates = new Vector<String>();
		for(Tag t : tags){
			if(curs.containsKey(t.word)){
				updates.add(t.word);
			}
		}
		Collections.sort(updates);
		if(!updates.isEmpty()){
			int len = updates.size();
			String[] items = new String[len];
			byte[][] values = new byte[len][];

			for(int i=0; i<len; i++){
				items[i] = updates.get(i);
				values[i] = Bytez.from(curs.get(items[i]));
			}

			this.setAttribute("tag", items, values);
		}
		
		//remove
		List<String> removes = new Vector<String>();
		for(Tag t : oldTags){
			if(!curs.containsKey(t.word)){
				removes.add(t.word);
			}
		}
		if(!removes.isEmpty()){
			this.deleteAttributes("tag", removes);
		}
	}	

	
	/**
	 * 设置标签
	 * @param tags
	 */
	public void setTags(List<Tag> tags){
		if(tags == null || tags.isEmpty())
			return;
		
		formatTags(tags);
		
		int len = tags.size();
		String[] items = new String[len];
		byte[][] values = new byte[len][];

		for(int i=0; i<len; i++){
			Tag tag = tags.get(i);
			items[i] = tag.word;
			values[i] = Bytez.from(Annealing.add(0, tag.weight, this.getAneCoef()));
		}
		this.setAttribute("tag", items, values);
		//检查是否超出最大标签维度
		checkTagDimension();
	}	
	
	/**
	 * 返回原始的标签数据 
	 * @return
	 */
	public List<Tag> getTags() {
//		List<Tag> tags = new Vector<Tag>();
//
//		for(String word:this.getAttributeItems("tag")){
//			Tag tag = new Tag();
//			tag.word = word;
//			tag.weight = Bytez.toDouble(this.getAttribute("tag", word));
//			tags.add(tag);
//		}
//		Collections.sort(tags);
//
//		return tags;
		
		return getAttributeItems("tag").stream()
				.map(item->new Tag(item, Bytez.toDouble(this.getAttribute("tag", item))))
				.sorted().collect(Collectors.toList());
	}
	
	/**
	 * 转换为一个长度 = 1的标准向量
	 * @return
	 */
	public List<Tag> getNormalTags() {
		List<Tag> tags = getTags();
		
//		double total = 0;
//		for(Tag tag : tags){
//			total += tag.weight*tag.weight;
//		}
//		
//		if(total == 0)total = 1;
//		total = Math.sqrt(total);
//		
//		for(Tag tag : tags){
//			tag.weight /= total;
//		}
//		
//		return tags;
		
		double total = tags.stream().map(t->t.weight*t.weight).reduce(0.0, (x,y)->x+y);
		if(total == 0)total = 1;
		
		final double length = Math.sqrt(total);;
		
		tags.forEach(t->t.weight/=length);
		
		return tags;
	}	
	
	/**
	 * 获取比重最大的几个维度标签
	 * @param size
	 * @return
	 */
	public List<Tag> getShowTags(int size){
		List<Tag> tags = this.getTags();
		Collections.sort(tags);
		
		tags = tags.subList(0, Math.min(size, tags.size()));
		for(Tag t : tags){
			t.weight = Annealing.anneValue(t.weight, this.getAneCoef());
		}
		
		return tags;
	}
	
	/**
	 * 得到退火后的标签
	 * @return
	 */
	public List<Tag> getAnneTags(){
		List<Tag> tags = this.getTags();
		for(Tag t : tags){
			t.weight = Annealing.anneValue(t.weight, this.getAneCoef());
		}
		
		return tags;
	}
	
	public double similar(RecommendEntity other){
		return VectorUtil.calcSimilar(getTags(), other.getTags());
	}
	
	
	public static double calcSimilar(RecommendEntity a, RecommendEntity b){
		return VectorUtil.calcSimilar(a.getTags(), b.getTags());
	}
	
	/**
	 * 排序截取标签维度
	 * @param tags
	 */
	private void formatTags(List<Tag> tags){
		Collections.sort(tags);

		for(int i=tags.size()-1; i > this.getTagDimension()-1; i--){
			tags.remove(i);
		}
	}
	
	private void checkTagDimension(){
		List<Tag> tags = this.getTags();
		if(tags.size() <= this.getTagDimension())
			return;
	
		Collections.sort(tags);
		List<String> removes = new Vector<String>();
		for(int i=this.getTagDimension(); i<tags.size(); i++ ){
			removes.add(tags.get(i).word);
		}
		
		this.deleteAttributes("tag", removes);
	}
	
}
