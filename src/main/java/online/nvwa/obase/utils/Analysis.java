package online.nvwa.obase.utils;

import online.nvwa.obase.data.Bytez;
import online.nvwa.obase.data.Entity;
import org.apache.hadoop.hbase.client.Table;

import java.util.List;

public class Analysis extends Entity {
	public Analysis() {
		super(Bytez.from(System.currentTimeMillis()));
	}
	
	public long getUserId(){
		return Bytez.toLong(this.getRow());
	}

	@Override
	public Table getBasisTable() {
		return null;
	}

	/**
	 *String request
	 */
	public void setRequest(String value){
		this.setAttribute("request", Bytez.from(value));
	}
	public String getRequest(){
		byte[] bytes = this.getAttribute("request");
		if(bytes == null)
			return "";
		return Bytez.toString(bytes);
	}	
	
	public String getRefer(){
		return this.getHeader("Refer");
	}
	
	/**
	 *String url
	 */
	public void setUrl(String value){
		this.setAttribute("url", Bytez.from(value));
	}
	public String getUrl(){
		byte[] bytes = this.getAttribute("url");
		if(bytes == null)
			return "";
		return Bytez.toString(bytes);
	}	
	
	/**
	 *String url
	 */
	public void setIp(String value){
		this.setAttribute("ip", Bytez.from(value));
	}
	public String getIp(){
		byte[] bytes = this.getAttribute("ip");
		if(bytes == null)
			return "";
		return Bytez.toString(bytes);
	}		

	/**
	 *String stub
	 */
	public void setStub(String value){
		if(value == null)value = "";
		this.setAttribute("stub", Bytez.from(value));
	}
	public String getStub(){
		byte[] bytes = this.getAttribute("stub");
		if(bytes == null)
			return "";
		return Bytez.toString(bytes);
	}		

	/**
	 *String site
	 */
	public void setSite(String value){
		this.setAttribute("site", Bytez.from(value));
	}
	public String getSite(){
		byte[] bytes = this.getAttribute("site");
		if(bytes == null)
			return "";
		return Bytez.toString(bytes);
	}	

	/**
	 *Headers
	 */
	public void setHeader(String item, String value){
		this.setAttribute("header", item,  Bytez.from(value));
	}
	public String getHeader(String item){
		byte[] bytes = this.getAttribute("header", item);
		if(bytes == null)
			return "";
		return Bytez.toString(bytes);
	}
	public List<String> getHeaders(){
		return this.getAttributeItems("header");
	}
	
	/**
	 *Cookies
	 */
	public void setCookie(String item, String value){
		this.setAttribute("cookie", item,  Bytez.from(value));
	}
	public String getCookie(String item){
		byte[] bytes = this.getAttribute("cookie", item);
		if(bytes == null)
			return "";
		return Bytez.toString(bytes);
	}
	public List<String> getCookies(){
		return this.getAttributeItems("cookie");
	}
	
	/**
	 *Params
	 */
	public void setParam(String item, String value){
		this.setAttribute("param", item,  Bytez.from(value));
	}
	public String getParam(String item){
		byte[] bytes = this.getAttribute("param", item);
		if(bytes == null)
			return "";
		return Bytez.toString(bytes);
	}
	public List<String> getParams(){
		return this.getAttributeItems("param");
	}	
	
	public String getDomain(){
		String domain = this.getHeader("Origin");
		if(domain == null){
			domain = this.getHeader("origin");
		}
		
		return domain;
	}
}
