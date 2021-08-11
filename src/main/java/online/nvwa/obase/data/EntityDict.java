package online.nvwa.obase.data;

import online.nvwa.obase.data.index.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityDict {
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
	
	private JsonEntity entity;
	private String name;
	public EntityDict(JsonEntity entity, String name){
		this.entity = entity;
		this.name = name;
	}
	public List<String> keys(){
		return entity.getAttributeItems(name);
	}	
	public Map<String, Value> get(){
		Map<String, Value> map = new HashMap<String, Value>();
		
		for(String key : keys()){
			map.put(key, get(key));
		} 
		return map;
	}
	
	public void put(String key, Value value){
		entity.setAttribute(name, key, value.encode());
	}
	
	public Value get(String key){
		return Value.decode(entity.getAttribute(name, key));
	}
	
	public void del(String key){
		entity.deleteAttribute(name, key);
	}

	public void del(){
		entity.deleteAttributeItems(name);
	}
	public boolean has(String key){
		return entity.getAttribute(name, key) != null;
	}
}
