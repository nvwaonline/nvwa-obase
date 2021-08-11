package online.nvwa.obase.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import online.nvwa.obase.data.Bytez;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Util {
	private static ExecutorService executer = Executors.newFixedThreadPool(5);
	public static ExecutorService threadPool(){
		return executer;
	}
	
	private static long serialId = 0;
	synchronized public static long getSerialLong(){
		return ++serialId;
	}

	//序列号
	private final static Object lockInt = new Object();
	private static int serialInt = 0;
	public static int getSerialInt(){
		synchronized(lockInt){
			serialInt ++;
			if(serialInt == Integer.MAX_VALUE)
				serialInt = 0;
			return serialInt;		
		}
	}	

	public static long getLeftTime(){
		return Long.MAX_VALUE - System.currentTimeMillis();
	}
	public static long getLeftTime(long time){
		return Long.MAX_VALUE - time;
	}

	//二进制反转
	public static long reverse(long i) {  
		// HD, Figure 7-1  
		i = (i & 0x5555555555555555L) << 1 | (i >>> 1) & 0x5555555555555555L;  
		i = (i & 0x3333333333333333L) << 2 | (i >>> 2) & 0x3333333333333333L;  
		i = (i & 0x0f0f0f0f0f0f0f0fL) << 4 | (i >>> 4) & 0x0f0f0f0f0f0f0f0fL;  
		i = (i & 0x00ff00ff00ff00ffL) << 8 | (i >>> 8) & 0x00ff00ff00ff00ffL;  
		i = (i & 0x0000ffff0000ffffL) << 16 | (i >>> 16) & 0x0000ffff0000ffffL;  
		i = (i & 0x00000000ffffffffL) << 32 | (i >>> 32) & 0x00000000ffffffffL;  
		return i;  
	}  	
	
	//二进制反转
	public static int reverse(int i) {  
		return (int)(reverse((long)i)>>32);  
	}  	

	public static long reverseOld(long i) {  
		// HD, Figure 7-1  
		i = (i & 0x5555555555555555L) << 1 | (i >>> 1) & 0x5555555555555555L;  
		i = (i & 0x0f0f0f0f0f0f0f0fL) << 4 | (i >>> 4) & 0x0f0f0f0f0f0f0f0fL;  
		i = (i & 0x00ff00ff00ff00ffL) << 8 | (i >>> 8) & 0x00ff00ff00ff00ffL;  
		i = (i << 48) | ((i & 0xffff0000L) << 16) |  
				((i >>> 16) & 0xffff0000L) | (i >>> 48);  
		return i;  
	}  	


	private static Map<String, Object> locks = new LruMap<String, Object>(10000);
	private static Object getLock(String name){
		if(!locks.containsKey(name)){
			locks.put(name, name);
		}

		return locks.get(name);
	}

	public static int getDiscreteIntID(Table table, String counter){   
		counter = table.getName().getNameWithNamespaceInclAsString()+"|"+counter;
		synchronized(getLock(counter)){
			try{
				Increment inc = new Increment(Bytes.toBytes(counter));
				inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("discrete") ,1);

				Result res = Obase.getIncrementTable().increment(inc);
				List<Cell> cells = res.listCells();

				long uid = Bytes.toLong(CellUtil.cloneValue(cells.get(0)));
				return (int)Util.reverse(uid<<32);
			}catch(Exception e){
				e.printStackTrace();
				return 0;
			}
		}
	}

	public static long getDiscreteID(Table table, String counter){   
		counter = table.getName().getNameWithNamespaceInclAsString()+"|"+counter;
		synchronized(getLock(counter)){
			try{
				Increment inc = new Increment(Bytes.toBytes(counter));
				inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("discrete") ,1);

				Result res = Obase.getIncrementTable().increment(inc);
				List<Cell> cells = res.listCells();

				long uid = Bytes.toLong(CellUtil.cloneValue(cells.get(0)));
				return Util.reverse(uid<<1);
			}catch(Exception e){
				e.printStackTrace();
				return 0;
			}
		}
	}

	public static int parseInt(String text){
		if(text == null)
			return 0;
		text = text.replace(",", "");
		text = text.toLowerCase();

		if(text.endsWith("k")){
			return (int)(Double.parseDouble(text.substring(0, text.length()-1))*1000);
		}

		return Integer.parseInt(text);
	}

	public static void clearTableCounters(Table table){  
		try{
			String prefix = table.getName().getNameWithNamespaceInclAsString()+"|";
			Scan scan = new Scan();
			Filter pf = new PrefixFilter(Bytez.from(prefix));
			scan.setFilter(pf);

			List<Delete> deletes = new Vector<Delete>();
			ResultScanner rs = Obase.getIncrementTable().getScanner(scan);
			for(Result r : rs){
				deletes.add(new Delete(r.getRow()));
			}

			Obase.getIncrementTable().delete(deletes);
		}catch(Exception e){
			e.printStackTrace();
		}
	}	


	public static long[] getDiscreteIDs(Table table, String counter, int count){   
		counter = table.getName().getNameWithNamespaceInclAsString()+"|"+counter;
		synchronized(getLock(counter)){
			if(count <= 0)return new long[0];
			try{
				Increment inc = new Increment(Bytes.toBytes(counter));
				inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("discrete") ,count);

				Result res = Obase.getIncrementTable().increment(inc);
				List<Cell> cells = res.listCells();

				long uid = Bytes.toLong(CellUtil.cloneValue(cells.get(0)));
				long[] ids = new long[count];
				for(int i=0;i<count; i++){
					ids[i] = uid-count+i+1;
					ids[i] = Util.reverse(ids[i]<<1);
				}
				return ids;
			}catch(Exception e){
				e.printStackTrace();
				return new long[0];
			}
		}
	}

	public static void clearDiscreteID(Table table, String counter) throws IOException{   
		counter = table.getName().getNameWithNamespaceInclAsString()+"|"+counter;
		synchronized(getLock(counter)){
			Put inc = new Put(Bytes.toBytes(counter));
			inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("discrete") ,Bytes.toBytes(0L));
			Obase.getIncrementTable().put(inc);
		}
	}
	public static long getOrderID(Class clazz, String counter){
		return getOrderID(Obase.getTable(clazz), counter);
	}	

	public static long getOrderID(Table table, String counter){  
		counter = table.getName().getNameWithNamespaceInclAsString()+"|"+counter;
		synchronized(getLock(counter)){
			try{
				Increment inc = new Increment(Bytes.toBytes(counter));
				inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("order") ,1);
				Result res = Obase.getIncrementTable().increment(inc);

				List<Cell> cells = res.listCells();

				long uid = Bytes.toLong(CellUtil.cloneValue(cells.get(0)));
				return uid;
			}catch(Exception e){
				e.printStackTrace();
				return 0;
			}
		}
	}

	public static long getOrderID(String counter){
		try{
			return Obase.getIncrementTable().incrementColumnValue(Bytez.from(counter),Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("order") ,1);
		}catch(Exception e){
			e.printStackTrace();
			return 0;
		}
	}


	public static long[] getOrderIDs(Table table, String counter, int count){
		counter = table.getName().getNameWithNamespaceInclAsString()+"|"+counter;
		synchronized(getLock(counter)){
			if(count <= 0)return new long[0];

			try{
				Increment inc = new Increment(Bytes.toBytes(counter));
				inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("order") ,count);
				Result res = Obase.getIncrementTable().increment(inc);

				List<Cell> cells = res.listCells();

				long uid = Bytes.toLong(CellUtil.cloneValue(cells.get(0)));
				long[] ids = new long[count];
				for(int i=0;i<count; i++){
					ids[i] = uid-count+i+1; 
				}
				return ids;
			}catch(Exception e){
				e.printStackTrace();
				return new long[0];
			}
		}
	}

	public static void clearOrderID(Table table, String counter) throws IOException{   
		clearOrderID(table, counter, 0);
	}
	public static void clearOrderID(Table table, String counter, long value) throws IOException{
		counter = table.getName().getNameWithNamespaceInclAsString()+"|"+counter;
		synchronized(getLock(counter)){
			Put inc = new Put(Bytes.toBytes(counter));
			inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("order") , Bytes.toBytes(value));
			Obase.getIncrementTable().put(inc);
		}
	}

	public static void clearOrderID(String counter){
		synchronized(getLock(counter)){
			try {
				Put inc = new Put(Bytes.toBytes(counter));
				inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR), Bytes.toBytes("order"), Bytes.toBytes(0L));
				Obase.getIncrementTable().put(inc);
			}catch (Exception e){
				e.printStackTrace();
			}
		}
	}


	public static void setGlobalValue(String valueName, byte[] value){
		Put inc = new Put(Bytes.toBytes(valueName));
		inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("value") , value);
		try {
			Obase.getIncrementTable().put(inc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	

	public static byte[] getGlobalValue(String valueName){
		Get inc = new Get(Bytes.toBytes(valueName));
		inc.addColumn(Bytes.toBytes(Obase.FAMILY_ATTR),Bytes.toBytes("value"));

		Result r = null;
		try {
			r = Obase.getIncrementTable().get(inc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		List<Cell> cells = r.listCells();
		if(cells == null || cells.isEmpty())
			return null;

		return CellUtil.cloneValue(cells.get(0));
	}	

	/**
	 * 生成预分区数据
	 * @param partition
	 * @return
	 */
	public static byte[][] calcSplitKeys(int partition) {  
		byte[][] splitKeys = new byte[partition][];
		for(int i=0; i<partition; i++){
			splitKeys[i] = Bytes.toBytes(Util.reverse(Long.MAX_VALUE/(partition+1)*(i+1)));  
		}
		return splitKeys;  
	}  



	public static byte[] bytesFromFile(String filePath) throws Exception{
		FileInputStream fis = new FileInputStream(filePath);

		return bytesFromInputStream(fis);
	}
	public static byte[] bytesFromFile(File file) throws Exception{
		FileInputStream fis = new FileInputStream(file);

		return bytesFromInputStream(fis);
	}

	public static byte[] bytesFromInputStream(InputStream is) throws Exception{
		BufferedInputStream bis = new BufferedInputStream(is);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		byte[] buffer = new byte[1024];
		int len = 0;
		while((len = bis.read(buffer)) > 0){
			baos.write(buffer, 0, len);
		}
		bis.close();

		return baos.toByteArray();
	}

	/**
	 * 更加索引行号获得索引的行键 
	 * @param startRow
	 * @return
	 */
	public static byte[] getIndexRow(byte[] startRow){
		return Bytes.add(startRow, Bytes.toBytes("i"));
	}



	//服务端调用
	public static byte[] invoke(Object o, byte[] bytes) throws Exception{
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		ObjectInputStream ois = new ObjectInputStream(bais);
		return invoke(o, ois);		
	}
	
	//服务端调用
	public static byte[] invoke(Object o, ObjectInputStream ois) throws Exception{
		String method = ois.readUTF();
		int pnum = ois.readInt();
		Class<?>[] pts = new Class[pnum];
		for(int i=0; i<pnum; i++){
			pts[i] = Class.forName(ois.readUTF());
		}
		pnum = ois.readInt();
		Object[] args = new Object[pnum];
		for(int i=0; i<pnum; i++){
			args[i] = ois.readObject();
		}
		Method m = o.getClass().getMethod(method, pts);
		Object ret = m.invoke(o, args);
		
		if(ret == null)
			return null;
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(ret);
		oos.close();
		return baos.toByteArray();
	}

	/**
	 * 针对实现了RegionOperationObserve的类执行服务端region操作调用
	 *
	 * @param entityClass 所在类
	 * @param startKey region start key
	 * @param method 方法名
	 * @return 成功或失败
	 * @throws Exception
	 */
	public static boolean execRegionOperation(Class entityClass, byte[] startKey, String method, byte[] splitPoint) throws Exception{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeUTF(method);
		if(splitPoint != null && splitPoint.length >0) {
			oos.writeObject(splitPoint);
		}
		oos.close();

		Get get = new Get(startKey);
		get.addColumn(Bytez.from(Obase.FAMILY_EXEC_REGION), baos.toByteArray());
		Result r = Obase.getTable(entityClass).get(get);
		if(r==null || r.isEmpty())
			throw new Exception("exec region operation failed, maybe this table not implemented RegionOperationObserver");

		boolean ret = false;
		String cause = "";
		List<Cell> cells = r.listCells();
		if(cells == null)return false;
		for(Cell c : cells){
			String column = Bytez.toString(CellUtil.cloneQualifier(c));
			if("ret".equals(column)) {
				ret = Bytez.toBoolean(CellUtil.cloneValue(c));
			}
			else if("cause".equals(column)){
				cause = Bytez.toString(CellUtil.cloneValue(c));
			}
		}

		if(ret)return true;

		if(cause != null && cause.length() > 0){
			throw new Exception(cause);
		}

		throw new Exception("exec region operation failed, unknown reson.");
	}

	public static boolean execRegionOperation(Class entityClass, byte[] startKey, String method, Map<String,String> paras) throws Exception{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeUTF(method);
		oos.writeObject(paras);
		oos.close();

		Get get = new Get(startKey);
		get.addColumn(Bytez.from(Obase.FAMILY_EXEC_REGION), baos.toByteArray());
		Result r = Obase.getTable(entityClass).get(get);
		if(r==null || r.isEmpty())
			throw new Exception("exec region operation failed, maybe this table not implemented RegionOperationObserver");

		boolean ret = false;
		String cause = "";
		List<Cell> cells = r.listCells();
		if(cells == null)return false;
		for(Cell c : cells){
			String column = Bytez.toString(CellUtil.cloneQualifier(c));
			if("ret".equals(column)) {
				ret = Bytez.toBoolean(CellUtil.cloneValue(c));
			}
			else if("cause".equals(column)){
				cause = Bytez.toString(CellUtil.cloneValue(c));
			}
		}

		if(ret)return true;

		if(cause != null && cause.length() > 0){
			throw new Exception(cause);
		}

		throw new Exception("exec region operation failed, unknown reson.");
	}

	public static boolean checkEmail(String email){
		if (!email.matches("[\\w\\.\\-]+@([\\w\\-]+\\.)+[\\w\\-]+")) {
			return false;
		}
		return true;
	}	

	public static List<String> searchList(String request){
		List<String> rets = new Vector<String>();
		if(request == null)return rets;

		String[] segs = request.split(" ");
		Hashtable<String, Integer> maps = new Hashtable<String, Integer>(); 
		for(String w:segs){
			if(w == null || w.equals(""))
				continue;
			maps.put(w, 1);
		}

		rets.addAll(maps.keySet());

		return rets;
	}
	public static List<String> searchListSmart(String request){
		List<String> rets = new Vector<String>();
		if(request == null)return rets;

		Hashtable<String, Integer> maps = new Hashtable<String, Integer>(); 
		try {
			maps = Fenci.text2words(request);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		rets.addAll(maps.keySet());

		return rets;
	}


	public final static int TEXT_CHINESE = 1;
	public final static int TEXT_ENGLISH = 2;
	public final static int TEXT_MIXTURE = 3;

	public final static int textStyle(String text){
		if(text == null || text.equals(""))
			return TEXT_MIXTURE;

		int chineseCount = 0;
		int spaceCount = 0;
		for(char c : text.toCharArray()){
			if(c == ' ')
				spaceCount++;
			else if(isChinese(c))
				chineseCount++;			
		}

		if(chineseCount==0)
			return TEXT_ENGLISH;

		if(chineseCount+spaceCount == text.length())
			return TEXT_CHINESE;

		return TEXT_MIXTURE;
	}

	public final static boolean isChineseString(String text){
		return textStyle(text) != TEXT_ENGLISH;
	}
	private static boolean isChinese(char c) {
		boolean result = false;
		if (c >= 19968 && c <= 171941) {// 汉字范围 \u4e00-\u9fa5 (中文)
			result = true;
		}
		return result;
	}	

	public static String readTxtFile(String filePath){
		try{
			File file=new File(filePath);
			if(file.isFile() && file.exists()){ //判断文件是否存在
				InputStreamReader read = new InputStreamReader(
						new FileInputStream(file));//考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);
				StringBuffer sb = new StringBuffer();
				String lineTxt = null;
				while((lineTxt = bufferedReader.readLine()) != null){
					sb.append(lineTxt);
				}
				read.close();
				return sb.toString();
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		return "";
	}	
	public static void saveTxtFile(String filePath, String text){
		try{
			File file=new File(filePath);
			OutputStreamWriter writer = new OutputStreamWriter(
					new FileOutputStream(file));//考虑到编码格式
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			bufferedWriter.write(text);
			bufferedWriter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}	

	public static String pureString(String text){
		if(text == null)
			return null;
		text = text.trim();
		text = text.replace("\t", " ");

		for(int i=0; i<100; i++){
			text = text.replace("  ", " ");
		}
		return text;
	}

	public static String requestPage(String url){
		StringBuffer buffer = new StringBuffer();  
		try {
			URL newUrl = new URL(url);

			System.setProperty("sun.net.client.defaultConnectTimeout", "60000");  
			System.setProperty("sun.net.client.defaultReadTimeout", "60000");  

			HttpURLConnection hConnect = (HttpURLConnection) newUrl.openConnection();  

			hConnect.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)");		
			hConnect.setConnectTimeout(60000);
			hConnect.setRequestMethod("GET");	

			hConnect.connect();

			// 读取内容                
			BufferedReader rd = new BufferedReader(new InputStreamReader(  
					hConnect.getInputStream()));  
			int ch;  
			while ((ch = rd.read()) > -1)  
				buffer.append((char) ch);  
			String s = buffer.toString();  

			rd.close();  
			hConnect.disconnect();

			return s;
		} catch (Exception e) { 
			e.printStackTrace();
			return "";
		}
	}	

	public static String timeToStr(long time){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return sdf.format(new Date(time));
	}

	public static boolean isNumber(String str)
	{
		java.util.regex.Pattern pattern=java.util.regex.Pattern.compile("[0-9]*");
		java.util.regex.Matcher match=pattern.matcher(str);
		return match.matches();
	}

	public static long scanTableRowCounts(Table table) throws IOException{
		Scan scan = new Scan();
		Filter kof = new FirstKeyOnlyFilter();
		scan.setFilter(kof);

		ResultScanner rs = table.getScanner(scan);
		long count = 0;
		for(Result r: rs){
			if(r == null)break;
			count++;
		}

		return count;
	}



	//	public static String apacheGet(String url){
	//		HttpClient client = new HttpClient();  
	//		String response = null;  
	//		String keyword = null;  
	//		GetMethod postMethod = new GetMethod(url);  
	//
	//		try {  
	//			client.executeMethod(postMethod);  
	//			response = new String(postMethod.getResponseBodyAsString()  
	//					.getBytes("ISO-8859-1"));
	//		} catch (Exception e) {  
	//
	//			e.printStackTrace();  
	//		}  
	//		return response;  
	//	}	

	public static String getDomain(String url){
		if(url == null || url.length() == 0)
			return null;
		url = url.toLowerCase();
		if("null".equals(url))
			return null;
		try {
			URL hurl = new  URL(url);
			return hurl.getHost();// 获取主机名 
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
	}



	public static String where(String  className){  
		try {  
			Class<?> theClazz = Class.forName(className);  
			return where(theClazz);  
		} catch (ClassNotFoundException e) {  
			return "CLASS_NOT_FOUND:"+className;  
		}  
	}  

	/** 
	 * 获取类所有的路径 
	 * @param cls 
	 * @return 
	 */  
	public static String where(@SuppressWarnings("rawtypes") final Class cls) {  
		if (cls == null)throw new IllegalArgumentException("null input: cls");  
		URL result = null;  
		final String clsAsResource = cls.getName().replace('.', '/').concat(".class");  
		final ProtectionDomain pd = cls.getProtectionDomain();  
		if (pd != null) {  
			final CodeSource cs = pd.getCodeSource();  
			if (cs != null) result = cs.getLocation();  
			if (result != null) {  
				if ("file".equals(result.getProtocol())) {  
					try {  
						if (result.toExternalForm().endsWith(".jar") ||  
								result.toExternalForm().endsWith(".zip"))  
							result = new URL("jar:".concat(result.toExternalForm())  
									.concat("!/").concat(clsAsResource));  
						else if (new File(result.getFile()).isDirectory())  
							result = new URL(result, clsAsResource);  
					}  
					catch (MalformedURLException ignore) {}  
				}  
			}  
		}  
		if (result == null) {  
			final ClassLoader clsLoader = cls.getClassLoader();  
			result = clsLoader != null ?  
					clsLoader.getResource(clsAsResource) :  
						ClassLoader.getSystemResource(clsAsResource);  
		}  
		return result.toString();  
	} 

	public static void clearNameSpace(String server, String namespace) throws Exception{
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", server);

		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();

		for(TableName tn : admin.listTableNames()){
			//			System.out.println(tn.getNamespaceAsString());
			if(tn.getNamespaceAsString().equals(namespace)){
				if(admin.tableExists(tn)) {
					if(admin.isTableEnabled(tn)){
						admin.disableTable(tn);
					}
					admin.deleteTable(tn);
				}			
			}
		}
	}

	public static String combineSearchTag(String destination, String words){
		if(destination != null)destination = destination.toLowerCase();
		if(words != null)words = words.toLowerCase();

		String tag = "";
		if(destination != null){
			tag = "destination:"+destination;
		}
		tag+="|language:en";

		if(words == null)return tag;

		//		List<String> sws = Util.searchList(words);
		List<String> sws = Util.searchListSmart(words);

		Collections.sort(sws);
		for(String word : sws){
			if(tag.length() > 0){
				tag += "|";
			}
			tag += "word:"+word;
		}

		return tag;
	}

	public static boolean isEmpty(String text){
		return (text== null || text.length()==0);
	}

	private static Gson _gson = null;
	public static Gson gson(){
		if(_gson == null){
			GsonBuilder gsonBuilder = new GsonBuilder();
			gsonBuilder.disableHtmlEscaping();
			//			_gson = gsonBuilder.setPrettyPrinting().create();			
			_gson = gsonBuilder.create();			
		}
		return _gson;
	}
	
	private static Gson _gsonPretty = null;
	public static Gson gsonPretty(){
		if(_gsonPretty == null){
			GsonBuilder gsonBuilder = new GsonBuilder();
			gsonBuilder.disableHtmlEscaping();
			gsonBuilder.setPrettyPrinting();
			_gsonPretty = gsonBuilder.setPrettyPrinting().create();
			
		}
		return _gsonPretty;
	}

	public static java.sql.Date pastDate(){
		java.sql.Date nowTime = new java.sql.Date(System.currentTimeMillis());
		return java.sql.Date.valueOf(nowTime+"");
	}
	public static java.sql.Date pastDate(long time){
		java.sql.Date nowTime = new java.sql.Date(time);
		return java.sql.Date.valueOf(nowTime+"");
	}


	/**
	 * 计算单机不重复的纳秒时间。计算时间范围 < 公元2262年
	 * Obase.nanoTime()/1000000 = System.currentTimeMillis()
	 * @return 从1970-01-01 00：00：00开始计算的纳秒数
	 */
	private static long atomicMillis = 0;
	private static long atomicNanos = 0;
	synchronized public static long nanoTime(){
		long millis = System.currentTimeMillis();
		if(atomicMillis == millis){
			atomicNanos += 1;
		}else{
			atomicMillis = millis;
			/**
			 * 采用随机数初始化防止多节点时间碰撞
			 */
			atomicNanos = (long)(Math.random()*100000);
		}

		return millis*1000000 + atomicNanos;
	}
	public static long millis2Nano(long millis){
		return millis*1000000;
	}
	public static long nano2millis(long nanos){
		return nanos/1000000;
	}
}
