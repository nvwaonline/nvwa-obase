package online.nvwa.obase.data;

import online.nvwa.obase.data.index.Value;

public class EntityAttr {
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
	public EntityAttr(JsonEntity entity, String name){
		this.entity = entity;
		this.name = name;
	}
	
	public Value get(){
		return Value.decode(entity.getAttribute(name));
	}
	public void set(Value value){
		//update index
		if(entity.isIndexColumn(name)){
			entity.updateIndex(name, this.get(), value);
		}
		entity.setAttribute(name, value.encode());
	}
	public void del(){
		entity.deleteAttribute(name);
	}
}
