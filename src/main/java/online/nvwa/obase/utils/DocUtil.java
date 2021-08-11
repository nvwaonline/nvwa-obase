package online.nvwa.obase.utils;

import online.nvwa.obase.data.Bytez;
import online.nvwa.obase.data.SearchableEntity;
import online.nvwa.obase.data.Tag;
import online.nvwa.obase.data.index.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class DocUtil {
	public static List<Elite> searchWords(Class entityClass, String words){
		Indexer indexer = new Indexer(entityClass);

		List<String> sws = Util.searchList(words);
		for(String w : sws)indexer.addTag("word", Value.from(w));
		List<Elite> elites = indexer.list();
		if(elites == null || elites.isEmpty()){
			indexer = new Indexer(entityClass);
			sws = Util.searchListSmart(words);
			for(String w : sws)indexer.addTag("word", Value.from(w));
			elites = indexer.list();
		}	
		
		return elites;
	} 

	public static void sortWithWords(Table hostTable, List<Elite> elites, int maxCount){
		long docCount = Indexer.getRowCount(hostTable);
		for(Elite e : elites){
			int total = 1;
			for(Find f : e.getMatchs()){
				if(f.getData()!=null){
					total = Math.max(total, Bytez.toInt(f.getData(), 4));
				}
			}

			for(Find f : e.getMatchs()){
				if(f.getIdx() !=null && f.getData()!=null){
					double weight = 0.01 + Math.log(docCount*1.0/ f.getIdx().getReference());
					e.setScore(e.getScore() + weight* Bytez.toInt(f.getData())/total);
				}
			}
		}

		//4. 根据匹配度进行排序
		Collections.sort(elites, new Comparator<Elite>(){
			@Override
			public int compare(Elite arg0, Elite arg1) {
				if(arg0.getScore() > arg1.getScore())
					return -1;
				if(arg0.getScore() < arg1.getScore())
					return 1;

				return Bytez.compareTo(arg0.getRow(),arg1.getRow());
			}			
		});
		
		while(elites.size() > maxCount){
			elites.remove(elites.size()-1);
		}
	}

	public static void sortWithTags(Table hostTable, List<Elite> elites, List<Tag> tags, int maxCount){
		Map<byte[], Elite> map = new TreeMap<byte[], Elite>(Bytes.BYTES_COMPARATOR);	

		List<byte[]> rowBytes = new Vector<byte[]>();
		for(Elite v : elites){
			rowBytes.add(v.getRow());
			map.put(v.getRow(), v);
		}
		rowBytes = new Recommender(hostTable, rowBytes).similar(tags).top(maxCount);

		elites.clear();
		for(byte[] row : rowBytes){
			elites.add(map.get(row));
		}			
	}	
	

	public static  Hashtable<String, Integer> replaceContent(String oldStr, String newStr) {
		try{
			Hashtable<String, Integer> news = Fenci.text2words(newStr);
			Hashtable<String, Integer> olds = Fenci.text2words(oldStr);

			return subWords(news, olds);
		}catch(Exception e){
			e.printStackTrace();
			return new Hashtable<String, Integer>(); 
		}
	}

	public static  Hashtable<String, Integer> subWords(Hashtable<String, Integer> news, Hashtable<String, Integer> olds){
		for(String word : olds.keySet()){
			Integer value = news.getOrDefault(word, 0);
			int count = value - olds.get(word);
			if(count == 0){
				news.remove(word);
			}else{
				news.put(word, count);
			}
		}
		return news;
	}	
	public static  Hashtable<String, Integer> addWords(Hashtable<String, Integer> news, Hashtable<String, Integer> olds){
		for(String word : olds.keySet()){
			Integer value = news.getOrDefault(word, 0);
			int count = value + olds.get(word);
			if(count == 0){
				news.remove(word);
			}else{
				news.put(word, count);
			}
		}
		return news;
	}	
	public static Hashtable<String, Integer> mulWords(Hashtable<String, Integer> olds, int factor){

		for(String word : olds.keySet()){
			olds.put(word, olds.get(word)*factor);
		}
		return olds;
	}
	
	public static Hashtable<String, Integer> updateTextIndex(SearchableEntity doc, String oldText, String newText, int weight){
		Hashtable<String, Integer> dif = DocUtil.replaceContent(oldText, newText);
		dif = DocUtil.mulWords(dif, weight);
		try {
			DocUtil.adjustDocWords(doc, "word", dif);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dif;
	}	
	
	/**
	 * 索引更新算法流程
	 * 1。根据变动表组装词文信息；
	 * 2. 计算文档的词语总数；
	 * 3. 根据词根，获取词语引用数；
	 * 4. 给词文添加引用数信息；
	 * 5. 添加库中未包含的词根；
	 * 6. 根据文档ID与词根获取原有词频；
	 * 7. 整合计算新的词频；
	 * 8. 词频为0的删除, 并降低词根引用值1；
	 * 9. 不为0词频的写入更新；  
	 * @param doc
	 * @param column
	 * @param fix
	 * @throws Exception
	 */
	public static void adjustDocWords(SearchableEntity doc, String column, Hashtable<String, Integer> fix) throws Exception{
		//解释：词根：词语本身；文档：包含词语的文章；词文：包括词根、文档、词语在文档中出现的次数；
		int wordCount = 0;
		int count = fix.size();

		List<String> words = new Vector<String>();
		words.addAll(fix.keySet());
	
		//列对象
		Idc idc= Idc.getInstance(doc.getBasisTable(), column);
		idc.connect();
		
//		Lunnar lunnar = new Lunnar(5);
//		lunnar.submit(new Runnable(){
//			@Override
//			public void run() {
//				Result[] results = BSL.getIdxTable().get(idxGets);
//				int newIdxCount = 0;
//				for(Result r : results){
//					if(r.isEmpty())newIdxCount ++ ;
//				}
//			}
//		});
		
		WordInfo[] wordArray = new WordInfo[count];
		
		//1. 整理
		List<Get> idxGets = new Vector<Get>();
		List<Get> indexGets = new Vector<Get>();
		for(int i=0; i<words.size(); i++){
			String word =  words.get(i);
			WordInfo wi = new WordInfo();
//			wi.word = word;
			wi.count = fix.get(word);
			wi.ref = 0;
			
			wordArray[i] = wi;
			wordCount += fix.get(word);

			byte[] ranking = Value.from(word).ranking();
			byte[] idxRow = Bytez.add(Bytez.from(idc.getId()), ranking);
			byte[] indexRow = Bytez.add(Bytez.from(idc.getId()), ranking, doc.getRow());
			
			wi.idxRow = idxRow;
			wi.indexRow = indexRow;
			
			idxGets.add(new Get(idxRow));
			indexGets.add(new Get(indexRow));
		}
		
		wordCount += doc.getWordCount();
		
		//2. 读取
		Result[] idxResults = Obase.getIdxTable().get(idxGets);
		Result[] indexResults = Obase.getIndexTable().get(indexGets);

		//3. 处理
		List<Put> idxPuts = new Vector<Put>();
		List<Put> indexPuts = new Vector<Put>();
		List<Delete> idxDeletes = new Vector<Delete>();
		List<Delete> indexDeletes = new Vector<Delete>();

		for(int i=0; i<count; i++){
			WordInfo wi = wordArray[i];

			//1 .处理idx
			Result x= idxResults[i];
			if(!x.isEmpty()){
				Idx idx = new Idx(x);
				wi.idx = idx;
				wi.ref = idx.getReference();
			}
			
			//2 .处理index
			Result r= indexResults[i];
			if(!r.isEmpty()){
				Index index = new Index(r);
//				wi.index = index;
				wi.count += Bytez.toInt(index.getData());
				
				if(wi.count == 0){
					wi.ref -= 1;
				}
			}else{
				wi.ref += 1;
			}
			
			//3. 更新
			//index delete
			//index update
			if(wi.count == 0){
				Delete del = new Delete(wi.indexRow);
				indexDeletes.add(del);
			}else{
				Put put = new Put(wi.indexRow);
				byte[] data = Bytes.add(Bytez.from(wi.count),Bytez.from(wordCount));
				put.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("data"), Index.toData(data) );
				indexPuts.add(put);
			}
			//idx delete
			//idx update
			if(wi.ref <= 0 && wi.idx != null){
				Delete del = new Delete(wi.idxRow);
				idxDeletes.add(del);
			}else if(wi.ref > 0){
				if(wi.idx!=null &&  wi.idx.getReference() == wi.ref){
					
				}else{
					Put put = new Put(wi.idxRow);
					put.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from(Obase.COLUMN_REFERENCE), Bytez.from(wi.ref));
					idxPuts.add(put);
				}				
			}
		}		
		
		//4. 更新
		doc.setWordCount(wordCount);
		
		if(!indexPuts.isEmpty())
		Obase.getIndexTable().put(indexPuts);
		if(!indexDeletes.isEmpty())
		Obase.getIndexTable().delete(indexDeletes);

		if(!idxPuts.isEmpty())
		Obase.getIdxTable().put(idxPuts);
		if(!idxDeletes.isEmpty())
		Obase.getIdxTable().delete(idxDeletes);
	}	

	private final static class WordInfo{
		long ref;
		Idx idx;
		byte[] idxRow;
		int count;
		byte[] indexRow;
	}
}
