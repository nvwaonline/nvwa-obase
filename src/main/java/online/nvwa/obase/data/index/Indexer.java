package online.nvwa.obase.data.index;

import online.nvwa.obase.data.Bytez;
import online.nvwa.obase.data.Entity;
import online.nvwa.obase.utils.Obase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;
import java.util.stream.Stream;

public class Indexer {
	private Table table;
	private List<Idx> searchTags = new Vector<Idx>();
	private List<Scope> searchScopes = new Vector<Scope>();

	public Indexer(Class entityClass){
		this.table = Obase.getTable(entityClass);
	}

	public Indexer(Entity entity){
		this(entity.getClass());
	}

	public static void destroyTableIndex(Table table){
		if(table == null)
			return;

		byte[] startRow = null;
		while(true){
			List<Idc> idcs = Idc.getTableIdcs(table, startRow, 1000);
			if(idcs.isEmpty())
				return;

			Lunnar lunnar = new Lunnar();

			for(final Idc idc:idcs){
				lunnar.submit(new Runnable(){
					@Override
					public void run() {
						try {
							idc.deleteIndexes();
							idc.deleteIdxes();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						idc.delete();
					}					
				});

				startRow = idc.getRow();  
				idc.delete();
			}

			lunnar.waitForComplete();
		}
	}

	public void destroyColumnIndex(String column) throws Exception{
		if(table == null)
			return;

		Idc idc = Idc.getInstance(table, column);
		if(idc.needConnect())
			return;

		idc.deleteIndexes();
		idc.deleteIdxes();
		idc.delete();
	}


	public static long getRowCount(Table table){
		return tableRowCounter(table).getReference();
	}
	public static void setRowCount(Table table, long count){
		Idx idx = tableRowCounter(table);
		idx.setReference(count);
		if(idx.needConnect()){
			idx.connect();
		}
	}
	public static long incRowCount(Table table, int num){
		synchronized(table){
			Idx idx = tableRowCounter(table);
			idx.incReference(num);
			if(idx.needConnect()){
				idx.connect();
			}
			return idx.getReference();
		}
	}
	public static long decRowCount(Table table){
		synchronized(table){
			Idx idx = tableRowCounter(table);
			idx.decReference();
			if(idx.needConnect()){
				idx.connect();
			}
			return idx.getReference();
		}
	}

	public static void updateTableWordCount(Table table) throws Exception{
		Idc idc = Idc.getInstance(table, "word");
		if(idc.needConnect())
			return;

		Scan scan = new Scan(Bytez.from(idc.getId()), Bytez.from(idc.getId()+1));
		ResultScanner rs = Obase.getIndexTable().getScanner(scan);

		int finish = 0;
		for(Result r: rs){
			byte[] irow = r.getRow();
			Index index = new Index(r);
			int disp = 4+2; //idc 4bytes + string.len 2bytes
			disp += Bytez.toShort(irow, 4);
			byte[] row = Bytez.copy(irow, disp, irow.length-disp);

			int oldtotal = Bytez.toInt(index.getData(), 4);
			int count = Bytez.toInt(index.getData()); 
			String info = count+"/" + oldtotal + "--->";

			//wordCount Result
			Get get = new Get(row);
			get.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("wordCount"));
			Result wcr = table.get(get);
			Cell c = wcr.getColumnLatestCell(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("wordCount"));
			if(c != null){
				int total = Bytez.toInt( CellUtil.cloneValue(c));
				if(total != oldtotal){
					index.setData(Bytez.add(Bytez.from(count), Bytez.from(total)));
				}
				info += "--->"+count+"/"+total;
			}
			finish++;

			if(finish%100 == 0){
				System.out.println(finish + "\t" + info);
			}
		}
	}	

	public static void updateWordCountFast(Table table) throws Exception{
		Idc idc = Idc.getInstance(table, "word");
		if(idc.needConnect())
			return;

		Map<byte[], Integer> cache = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);

		Scan scan = new Scan(Bytez.from(idc.getId()), Bytez.from(idc.getId()+1));
		ResultScanner rs = Obase.getIndexTable().getScanner(scan);

		int finish = 0;
		for(Result r: rs){
			byte[] irow = r.getRow();
			Index index = new Index(r);
			int disp = 4+2; //idc 4bytes + string.len 2bytes
			disp += Bytez.toShort(irow, 4);
			byte[] row = Bytez.copy(irow, disp, irow.length-disp);

			int oldtotal = Bytez.toInt(index.getData(), 4);
			int count = Bytez.toInt(index.getData()); 
			String info = count+"/" + oldtotal + "--->";

			//wordCount Result
			if(!cache.containsKey(row)){
				Get get = new Get(row);
				get.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("wordCount"));
				Result wcr = table.get(get);
				Cell c = wcr.getColumnLatestCell(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("wordCount"));
				if(c != null){
					int total = Bytez.toInt( CellUtil.cloneValue(c));
					cache.put(row, total);
				}
			}
			int total = cache.getOrDefault(row, 1);

			if(total != oldtotal){
				index.setData(Bytez.add(Bytez.from(count), Bytez.from(total)));
			}
			info += "--->"+count+"/"+total;
			finish++;

			if(finish%100 == 0){
				System.out.println(finish + "\t" + info);
			}
		}
	}	


	/**
	 * 所有自动分配的Idc都具有>=1的id，为0的ID刚好作为表名索引。
	 */
	private static Idx tableRowCounter(Table table){
		return new Idx(Bytez.add(Bytez.from((int)0), table.getName().toBytes()));
	}


	//得到索引词语在一个对象中出现的次数
	public int getIndexCount(byte[] row, String column, Value value) throws Exception{
		if(!checkIndex(row, column, value))
			return 0;

		Idc idc = Idc.getInstance(table, column);
		if(idc.needConnect())
			return 0;

		Index index = new Index(idc, value, row);
		if(index.needConnect())
			return 0;

		return Bytez.toInt(index.getData());
	}

	/**
	 * 获取索引的附加数据
	 * @param row
	 * @param column
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public byte[] getIndexData(byte[] row, String column, Value value) throws Exception{
		if(!checkIndex(row, column, value))
			return null;

		Idc idc = Idc.getInstance(table, column);
		if(idc.needConnect())
			return null;

		Index index = new Index(idc, value, row);
		if(index.needConnect())
			return null;

		return index.getData();
	}
	/**
	 * 设置索引的附加数据
	 * @param row
	 * @param column
	 * @param value
	 * @param data
	 */
	public void setIndexData(byte[] row, String column, Value value, byte[] data){
		if(!checkIndex(row, column, value))
			return;

		Idc idc = Idc.getInstance(table, column);
		if(idc.needConnect())
			return;

		Index index = new Index(idc, value, row);
		index.setData(data);
		index.connect();
	}

	public void replaceIndex(byte[] row, String column, Value value, Value value2){
		replaceIndex(row, column, value, value2, null);
	}

	public void replaceIndex(byte[] row, String column, Value value, Value value2, byte[] data){
		if(value == value2)return;
		if(value!=null&&value2!=null&&value.equal(value2))
			return;

		try{
			this.removeIndex(row, column, value);
			this.addIndex(row, column, value2, data);
			this.submit();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	/**
	 * 添加搜索标签
	 * @param column
	 * @param value
	 * @return
	 */
	public Indexer addTag(String column, Value value){
		if(value == null || value.equals(""))
			return this;

		Idc idc = Idc.getInstance(table, column);
		try {
			searchTags.add(new Idx(idc, value));
		} catch (Exception e) {
			e.printStackTrace();
		}

		return this;
	}

	/**
	 * 添加搜索范围
	 * @param column
	 * @param start
	 * @param stop
	 * @return
	 */
	public Indexer addScope(String column, Value start, Value stop){
		if(start == null || stop==null)
			return this;

		Idc idc = Idc.getInstance(table, column);
		try {
			searchScopes.add(new Scope(idc, start, stop));
		} catch (Exception e) {
			e.printStackTrace();
		}

		return this;
	}

	/**
	 * 添加索引
	 * @param row
	 * @param column
	 * @param value
	 * @return
	 */
	public Indexer addIndex(byte[] row, String column, Value value){
		return addIndex(row, column, value, null);
	}

	/**
	 * 调整文档中某个标签的次数
	 * @param row
	 * @param column
	 * @param value
	 * @param count
	 * @return
	 */
	public Indexer adjustIndex(byte[] row, String column, Value value, int count){
		//		if(!checkIndex(row, column, value))
		//			return this;
		try{
			Idc idc= Idc.getInstance(table, column);
			if(idc.needConnect()){
				idc.connect();
			}

			Idx idx = new Idx(idc, value);

			Index index = new Index(idc, value, row);
			index.setData( Bytez.from(Bytez.toInt(index.getData()) + count));
			if(index.needConnect()){
				index.connect();
				idx.incReference(1);
			}

			if(Bytez.toInt(index.getData()) == 0){
				index.delete();
				idx.decReference();
			}
		}catch(Exception e){
			e.printStackTrace();
		}


		return this;
	}

	/**
	 * 根据索引查询某列等于某值的所有记录的行键
	 * @param entityClass
	 * @param column
	 * @param value
	 * @return
	 */
	public static Stream<IndexData> find(Class entityClass, String column, Value value){
		Idc idc= Idc.getInstance(Obase.getTable(entityClass), column);
		Idx idx = new Idx(idc, value);
		return idx.stream();
	}

	/**
	 * 根据索引查询某列值处于某一范围内的所有记录的行键
	 * @param entityClass
	 * @param column
	 * @param start
	 * @param stop
	 * @return
	 */
	public static Stream<IndexData> find(Class entityClass, String column, Value start, Value stop){
		Idc idc= Idc.getInstance(Obase.getTable(entityClass), column);
		Scan scan = new Scan().withStartRow(new Idx(idc, start).getRow()).withStopRow(new Idx(idc, stop).getRow());

//        Bytes.copy(e.getRow(),irow.length,e.getRow().length-irow.length),

        byte[] irow = new Idx(idc, start).getRow();


//        return Index.stream(scan).map(e->new IndexData(e.getRow(), e.getData()));
        return Index.stream(scan).map(e->new IndexData(
                Bytes.copy(e.getRow(),irow.length,e.getRow().length-irow.length),
                e.getData()));
	}

	/**
	 * 添加索引
	 * @param row 行
	 * @param column 列
	 * @param value 值
	 * @param data 附加数据
	 * @return
	 */
	public Indexer addIndex(byte[] row, String column, Value value, byte[] data){
		if(!checkIndex(row, column, value))
			return this;


		Idc idc= Idc.getInstance(table, column);
		if(idc.needConnect()){
			idc.connect();
		}

		Index index = new Index(idc, value, row);
		index.setData(data);
		if(index.needConnect()){
			index.connect();
			try{
				Idx idx = new Idx(idc, value);
				idx.connect();
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		return this;
	}

	/**
	 * 移除索引
	 * @param row
	 * @param column
	 * @param value
	 * @return
	 */
	public Indexer removeIndex(byte[] row, String column, Value value){
		if(!checkIndex(row, column, value))
			return this;

		Idc idc= Idc.getInstance(table, column);
		if(!idc.needConnect()){
			Index index = new Index(idc, value, row);
			if(!index.needConnect()){
				index.delete();
			}
		}

		return this;
	}	

	/**
	 * 索引，相当于搜索
	 * @return 匹配的行集合
	 */
	public List<Elite> list(){
		submit();

		Map<byte[], Elite> elites = new TreeMap<byte[], Elite>(Bytes.BYTES_COMPARATOR);	

		if(searchTags.isEmpty() && searchScopes.isEmpty())
			return new Vector<Elite>();

		for(Scope scope : searchScopes){
			if(scope.idc.needConnect() ||scope.start ==null||scope.stop==null||scope.start.getType() != scope.stop.getType()){
				return new Vector<Elite>();
			}
		}

		for(Idx idx : searchTags){
			if(idx.needConnect()){
				return new Vector<Elite>();
			}
		}

		Collections.sort(searchTags);

		long start = System.currentTimeMillis();

		List<Find> finds = null;
		for(Idx idx : searchTags){
			if(finds == null){
				finds = idx.getIndexRows();
			}else{
				finds = idx.filterIndexRows(finds);
			}

			if(Obase.DebugIndex)
				System.out.println("time = " + (System.currentTimeMillis()-start));

			if(finds == null || finds.isEmpty())
				return new Vector<Elite>();

			for(Find find : finds){
				if(!elites.containsKey(find.getRow())){
					elites.put(find.getRow(), new Elite(find.getRow()));
				}
				elites.get(find.getRow()).addMatch(find);
			}
			if(Obase.DebugIndex)
				System.out.println("time = " + (System.currentTimeMillis()-start));

			if(Obase.DebugIndex)
				System.out.println("find" + idx.getName() +"; reference = " + idx.getReference() + "; matches " +finds.size());
		}		

		for(Scope scope : searchScopes){
			if(finds == null){
				finds = scope.getIndexRows();
			}else{
				finds = scope.filterIndexRows(finds);
			}
			if(Obase.DebugIndex)
				System.out.println("time = " + (System.currentTimeMillis()-start));

			if(finds == null || finds.isEmpty())
				return new Vector<Elite>();

			for(Find find : finds){
				if(!elites.containsKey(find.getRow())){
					elites.put(find.getRow(), new Elite(find.getRow()));
				}
				elites.get(find.getRow()).addMatch(find);
			}
			if(Obase.DebugIndex)
				System.out.println("time = " + (System.currentTimeMillis()-start));

			if(Obase.DebugIndex)
				System.out.println("find scope matches " +finds.size());
		}		

		List<Elite> rets = new Vector<Elite>();
		for(Find m : finds){
			rets.add(elites.get(m.getRow()));
		}

		return rets;
	}

	/**
	 * 提交索引
	 * 不存在的索引需要添加，存在的覆盖
	 */
	public void submit(){
	}

	private boolean checkIndex(byte[] row, String column, Value label){
		if(row == null || column == null || label == null)
			return false;

		if(column.equals("")||label.equals(Value.from("")))
			return false;

		return true;
	}
}
