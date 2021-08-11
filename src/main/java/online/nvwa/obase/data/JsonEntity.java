package online.nvwa.obase.data;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import online.nvwa.obase.data.index.Value;
import org.apache.hadoop.hbase.client.Result;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 文档类对象
 * @author Victor.Shaw
 * 具有创建时间、词语总数、被引用数次数属性，
 */
public abstract class JsonEntity extends TimeEntity {
	/*
	 * 保存对象的索引列, 为Json对象的哪些最上层列建立索引
	 */
	private static Map<String, Set<String>> indexColumns = new HashMap<String, Set<String>>();
	public static void addIndexColumn(String cname,String column){
		Set<String> cols = indexColumns.get(cname);
		if(cols == null){
			cols = new HashSet<String>();
			indexColumns.put(cname, cols);
		}
		cols.add(column);
	} 
	final public boolean isIndexColumn(String column){
		Set<String> idxCols = indexColumns.get(this.getClass().getName());
		if(idxCols!=null && idxCols.contains(column)){
			return true;
		}

		return false;
	}
	
	public JsonEntity(byte[] row) {
		super(row);
	}
	
	public JsonEntity(byte[] row, boolean autoload) {
		super(row, autoload);
	}	
	public JsonEntity(Result r) {
		super(r);
	}
	
	
	public JsonObject toJsonObject(){
		JsonObject jo = new JsonObject();
		
		for(Entry<byte[], byte[]> entry : this.getBasis().entrySet()){
			String key = Bytez.toString(entry.getKey());
			if("createTime".equals(key))continue;
			
			byte[] value = entry.getValue();
			int pos = key.indexOf(":");
			
			if(pos <= 0){				
				Value v = Value.decode(value);
				if(v.isBoolean())
					jo.addProperty(key , v.asBoolean());
				else if(v.isNumber())
					jo.addProperty(key , v.asNumber());
				else if(v.isString())
					jo.addProperty(key , v.asString());
				
				continue;
			}
			
			String name = key.substring(0, pos);
			String item = key.substring(pos+1);
			if(value.length <= 2){
				JsonArray list = jo.getAsJsonArray(name);
				if(list == null){
					list = new JsonArray();
					jo.add(name, list);
				}
				list.add(item);
			}else{
				JsonObject dict = jo.getAsJsonObject(name);
				if(dict == null){
					dict = new JsonObject();
					jo.add(name, dict);
				}
				Value v = Value.decode(value);
				if(v.isBoolean())
					dict.addProperty(key , v.asBoolean());
				else if(v.isNumber())
					dict.addProperty(key , v.asNumber());
				else if(v.isString())
					dict.addProperty(key , v.asString());
			}
		}
		
		return jo;		
	}
	
	public void saveJsonObject(JsonObject jo){		
		for(Entry<String, JsonElement> entry : jo.entrySet()){
			String name = entry.getKey();
			if("createTime".equals(name))continue;
			JsonElement value = entry.getValue();
			
			if(value.isJsonPrimitive()){
				JsonPrimitive jp = value.getAsJsonPrimitive();
				Value v = primitive2value(jp);				
				this.attr(name).set(v);				
			}else if(value.isJsonArray()){
				JsonArray list = value.getAsJsonArray();
				for(JsonElement je : list){
					if(je.isJsonPrimitive()){
						String item = primitive2string(je.getAsJsonPrimitive());
						this.list(name).add(item);
					}					
				}				
			}else if(value.isJsonObject()){
				JsonObject dict = value.getAsJsonObject();
				for(Entry<String, JsonElement> e : dict.entrySet()){
					if(e.getValue().isJsonPrimitive()){
						JsonPrimitive jp = e.getValue().getAsJsonPrimitive();
						String item = e.getKey();
						this.dict(name).put(item, primitive2value(jp));
					}					
				}	
			}
		}
	}
	
	private static Value primitive2value(JsonPrimitive jp){
		if(jp.isBoolean())
			return Value.from(jp.getAsBoolean());
		if(jp.isNumber())
			return Value.from(jp.getAsDouble());
		if(jp.isString())
			return Value.from(jp.getAsString());
		
		return null;
	}
	private static String primitive2string(JsonPrimitive jp){
		if(jp.isBoolean())
			return "" + jp.getAsBoolean();
		if(jp.isNumber())
			return "" + jp.getAsDouble();
		if(jp.isString())
			return jp.getAsString();
		
		return "";
	}
	
	/*
	item.attr("").get()
	item.attr("").set(byte[])
	item.attr("").del()
	item.list("").get() list<String>
	item.list("").add("")
	item.list("").del("")
	item.dict("").get()  map<String, byte[]>
	item.dict("").get("")  byte[]
	item.dict("").put("", value)
	item.dict("").del("")
	*/
	public EntityAttr attr(String name){return new EntityAttr(this, name);}
	public EntityList list(String name){return new EntityList(this, name);}
	public EntityDict dict(String name){return new EntityDict(this, name);}
}
