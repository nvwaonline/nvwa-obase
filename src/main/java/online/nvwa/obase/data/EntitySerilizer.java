package online.nvwa.obase.data;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class EntitySerilizer implements JsonSerializer<Entity> {
	@Override
	public JsonElement serialize(Entity src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject jo = new JsonObject();
		
		src.getBasis().entrySet().stream().forEach(e->jo.addProperty(Bytez.toString(e.getKey()), Bytez.toString(e.getKey())));
		
		return jo;
	}  
}  
