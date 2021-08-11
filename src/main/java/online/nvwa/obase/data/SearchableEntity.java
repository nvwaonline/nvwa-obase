package online.nvwa.obase.data;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 文档类对象
 * @author Victor.Shaw
 * 具有创建时间、词语总数、被引用数次数属性，
 */
public abstract class SearchableEntity extends ReferenceEntity {
	public SearchableEntity(byte[] row) {
		super(row);
	}

	public SearchableEntity(Result r) {
		super(r);
	}
	
	/**
	 * 文档的词语总数，用于计算词频
	 * @return
	 */
	public int getWordCount(){
		byte[] bytes = this.getAttribute("wordCount");
		if(bytes == null)
			return 0;

		return Bytes.toInt(bytes);
	}
	
	//会在分词时自动设置
	public void setWordCount(int value){
		this.setAttribute("wordCount", Bytes.toBytes(value));
	}
	
	/**
	 * long showTime
	 */
	public long getShowTime(){
		byte[] bytes = this.getAttribute("showTime");
		if(bytes == null)
			return 0;

		return Bytes.toLong(bytes);
	}
	public void setShowTime(long value){
		this.setAttribute("showTime", Bytes.toBytes(value));
	}

	/**
	 * long choiceTime
	 */
	public long getChoiceTime(){
		byte[] bytes = this.getAttribute("choiceTime");
		if(bytes == null)
			return 0;

		return Bytes.toLong(bytes);
	}
	public void setChoiceTime(long value){
		this.setAttribute("choiceTime", Bytes.toBytes(value));
	}
	
//	public void setText
	
}
