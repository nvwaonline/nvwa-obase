package online.nvwa.obase.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.net.URL;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;


/**
 * 骨架对象
 * @author Victor.Shaw
 * 用于从dom对象中抽取文本信息
 * 数据挖掘
 */
public class Xgrab {
	public final static String KEY_TYP = "FILTER_TYP";
	public final static String KEY_TAG = "FILTER_TAG";
	public final static String KEY_CLZ = "FILTER_CLZ";
	public final static String KEY_ATR = "FILTER_ATR";

	public boolean required = false;
	public boolean unique = false;
	
	public String tag; //标签类型
	public String clzes; //匹配class
	public JsonObject attrs; //匹配attr
	
	public Map<String, Object> props;
	
	/**
	 * 从文件加载骨架信息
	 * @param templateFile
	 * @return
	 */
	public static Xgrab load(String templateFile){
		String text = Util.readTxtFile(templateFile);
		JsonObject jo = Util.gson().fromJson(text, JsonObject.class);
		return Xgrab.parseProps(jo);
	}
	
	/**
	 * 从字串加载骨架信息
	 * @param json
	 * @return
	 */
	public static Xgrab parse(String json){
		JsonObject jo = Util.gson().fromJson(json, JsonObject.class);
		return Xgrab.parseProps(jo);
	}

	/**
	 * 通过代理从网页挖取信息
	 * @param proxy
	 * @param url
	 * @param wait
	 * @return
	 */
	public JsonObject extract(String proxy, String url, int wait){
		String home = Webage.loadPage(proxy, url, wait) ;	
		//转化为json对象
		Document doc = Jsoup.parse(home);
		return extract(doc);
	}
	
	/**
	 * 直接从网页挖取信息
	 * @param url
	 * @param wait
	 * @return
	 */
	public JsonObject extract(String url, int wait){
		try{
			Document doc = Jsoup.parse(new URL(url), wait);
			return extract(doc);
		}catch(Exception e){
			return null;
		}
	}

	/**
	 * 从dom对象挖取信息
	 * @param node
	 * @return
	 */
	public JsonObject extract(Element node){
		JsonObject jo = new JsonObject();
		
		for(Entry<String, Object> entry : props.entrySet()){
			String name = entry.getKey();			
			Object value = entry.getValue();
			
			//转化单纯的属性类型
			if(value instanceof String){
				String attr = (String)value;
				if("text".equals(attr)){
					jo.addProperty(name, node.text());
				}else{
					jo.addProperty(name, node.attr(attr));
				}
			}
			//转化节点类型
			else if(value instanceof Xgrab){
				Xgrab grab = (Xgrab)value;

				List<Element> finds = node.getElementsByTag(grab.tag).stream()
						.filter(e->hasClazzes(e,grab.clzes))
						.filter(e->hasAttrs(e,grab.attrs))
						.collect(Collectors.toList());
				if(finds.isEmpty()){
					if(grab.required)return null;
					continue;
				}
				
				List<JsonObject> rets = finds.stream()
						.map(e->grab.extract(e))
						.filter(e->e!=null)
						.collect(Collectors.toList());
				if(rets.isEmpty() && grab.required)
					return null;

				//for unique
				if(grab.unique && rets.size() > 1)
					return null;
				
				if(rets.isEmpty())
					continue;
					
				
				if(name.endsWith("[]")){
					List<Object> simples = rets.stream()
							.map(e->simplify(e))
							.collect(Collectors.toList());
							
					jo.add(name.substring(0, name.length()-2), Util.gson().toJsonTree(simples));
				}else{
					JsonObject ret = rets.get(0); 

					JsonObject temp = new JsonObject();				
					for(Entry<String, JsonElement> et: ret.entrySet()){
						String key = et.getKey();
						/**
						 * 如果key以'_'开头，则把属性放到上一级对象中，并且拼接变量名
						 * 1. 以'_'开头，新的属性名为 name + key;
						 * 2. 以'_''_'开头，新的属性名为 name + key.substring(2);
						 */
						if(key.startsWith("_")){
							if(key.startsWith("__"))
								jo.add(name+key.substring(2), et.getValue());
							else 
								jo.add(name+key, et.getValue());
						}else{
							temp.add(key, et.getValue());
						}
					}
					if(temp.size() > 0){
						jo.add(name, temp);
					}				
				}
			}
		}
		
		return jo;
	}
	
	
	/**
	 * 从Json对象转化骨架信息
	 * @param jo
	 * @return 骨架
	 */
	private static Xgrab parseProps(JsonObject jo){
		Map<String, Object> props = new HashMap<String, Object>();
		for(Entry<String, JsonElement> entry : jo.entrySet()){
			switch(entry.getKey()){
			case KEY_TAG:
			case KEY_CLZ:
			case KEY_ATR:
			case KEY_TYP:
				continue;
			}
			
			if(entry.getValue().isJsonObject()){
				Xgrab grab = parseProps(entry.getValue().getAsJsonObject());
				props.put(entry.getKey(), grab);				
			}

			if(entry.getValue().isJsonPrimitive()){
				props.put(entry.getKey(), entry.getValue().getAsString());				
			}
		}
		
		Xgrab grab = new Xgrab();

		if(jo.has(KEY_TAG))
		grab.tag = jo.get(KEY_TAG).getAsString();
		if(jo.has(KEY_CLZ))
		grab.clzes = jo.get(KEY_CLZ).getAsString();
		if(jo.has(KEY_ATR))
		grab.attrs = jo.get(KEY_ATR).getAsJsonObject();
		if(jo.has(KEY_TYP)){
//			grab.required = "required".equals(jo.get(KEY_TYP).getAsString());
			Set<String> keys = Arrays.stream(jo.get(KEY_TYP).getAsString().split(","))
			.map(e->e.trim().toLowerCase())
			.collect(Collectors.toSet());
			
			if(keys.contains("required"))grab.required = true;
			if(keys.contains("unique"))grab.unique = true;
				
		}
		
		grab.props = props;

		return grab;
	} 
	
	/**
	 * 过滤class信息 
	 * @param node
	 * @param clazzes
	 * @return
	 */
	private static boolean hasClazzes(Element node, String clazzes){
		if(clazzes == null || clazzes.trim().length()==0)
			return true;
		
		String[] segs = clazzes.split(",| ");
		for(String seg : segs){
			if(!node.hasClass(seg.trim()))return false;
		}
		
		return true;
	} 
	
	/**
	 * 过滤attr信息
	 * @param node
	 * @param attrs
	 * @return
	 */
	private static boolean hasAttrs(Element node, JsonObject attrs){
		if(attrs== null || attrs.size()==0)
			return true;
		for(Entry<String, JsonElement> entry : attrs.entrySet()){
			String key = entry.getKey();
			if(!entry.getValue().isJsonPrimitive())
				return false;
			String value = entry.getValue().getAsString();

			String attr = "";
			if("text".equals(key)){
				attr = node.text();
			}else{
				if(!node.hasAttr(key))return false;
				attr = node.attr(key);
			}
			if(value == null || value.length() == 0)
				continue;

//			if(!value.equals(attr))return false;
	
			/**
			 * 用!表示非 # 表示前匹配和后匹配
			 * 如：
			 * 'abcd' 表示等于'abcd'
			 * '#abcd' 表示以'abcd' 结尾
			 * 'abcd#' 表示以'abcd' 开头
			 * '#abcd#' 表示包含'abcd'
			 * '!#abcd#' 表示不包含'abcd'
			 */			
			boolean reverse = value.startsWith("!");
			if(reverse)value = value.substring(1);
			boolean match = false;

			if(value.startsWith("#") && value.endsWith("#") && value.length()>2){
				value = value.substring(1, value.length()-1);
				match = attr.indexOf(value)>=0;
			}else if(value.startsWith("#")){
				value = value.substring(1);
				match = attr.endsWith(value);
			}else if(value.endsWith("#")){
				value = value.substring(0, value.length()-1);
				match = attr.startsWith(value);
			}else{
				match = attr.equals(value);
			}
			
			if(reverse)match = !match;			
			if(!match)return false;
		}
		
		return true;
	}
	
	private static JsonElement simplify(JsonObject jo){
		if(jo.size() == 1 && jo.has("__"))
			return jo.get("__");
		return jo;
	}
}
