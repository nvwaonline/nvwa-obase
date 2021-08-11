package online.nvwa.obase.data;

import java.util.List;

public class EntityList {
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
	public EntityList(JsonEntity entity, String name){
		this.entity = entity;
		this.name = name;
	}
	
	public List<String> get(){
		return entity.getAttributeItems(name);
	}
	public void add(String value){
		entity.setAttribute(name, value, Bytez.from(true));
	}
	public void del(String value){
		entity.deleteAttribute(name, value);
	}
	public void del(){
		entity.deleteAttributeItems(name);
	}
	public boolean has(String value){
		return get().contains(value);
	}
}
