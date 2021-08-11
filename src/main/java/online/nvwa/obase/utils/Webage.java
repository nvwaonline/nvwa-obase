package online.nvwa.obase.utils;

import java.net.URLEncoder;
import java.util.Base64;

public class Webage {
	/**
	 * 打开一个不可见页面
	 * @param host
	 * @param workPage
	 * @return
	 */
	public static String openPage(String host, String workPage){
		String page = host + "openPage";
		String para = "?url="+workPage;

		return Util.requestPage(page + para);
	}
	
	/**
	 * 打开一个标签页面
	 * @param host
	 * @param workPage
	 * @return
	 */
	public static String tabPage(String host, String workPage){
		String page = host + "tabPage";
		String para = "?url="+workPage;

		return Util.requestPage(page + para);
	}

	/**
	 * 关闭页面
	 * @param host
	 * @param workPage
	 * @return
	 */
	public static String exitPage(String host, String workPage){
		String page = host + "closePage";
		String para = "?url="+workPage;

		return Util.requestPage(page + para);
	}

	/**	
	 * 根据页面链接获取页面内容
	 * @param host
	 * @param workPage
	 * @param wait
	 * @return
	 */
	public static String getHtml(String host, String workPage, long wait){
		try{
			Thread.sleep(wait);
		}catch(Exception e){
			e.printStackTrace();
		}

		String page = host + "htmlPage";
		String para = "?url="+workPage;
		para += "&wait="+wait;
		
		return Util.requestPage(page + para);
	}
	
	public static String loadPage(String host, String url, long wait){
		try {
			url =  URLEncoder.encode(url, "utf-8");

			String page = host + "loadPage";
			String para = "?url="+url;
			para += "&wait="+wait;

			return Util.requestPage(page + para);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}		
	
	/**
	 * 页面跳转
	 * @param host
	 * @param workPage
	 * @param url
	 * @return
	 */
	public static String transferPage(String host, String workPage, String url){
		String page = host + "execPage";
		String para = "?url="+workPage;
		para += "&script=window.location.href=\""+url+"\"";
		
		String ret = Util.requestPage(page + para);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}
	
	/**
	 * 页面执行JS脚本
	 * @param host
	 * @param workPage
	 * @param script
	 * @return
	 */	
	public static String execScript64(String host, String workPage, String script){
		try{
			String enc = Base64.getEncoder().encodeToString(script.getBytes("utf-8"));

			String page = host + "execPage64";
			String para = "?url="+workPage;
			para += "&script="+enc;

			String ret = Util.requestPage(page + para);
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return ret;
		}catch(Exception ex){
			return "";
		}
	}
}
