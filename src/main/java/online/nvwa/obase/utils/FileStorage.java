package online.nvwa.obase.utils;

import online.nvwa.obase.data.Bytez;
import online.nvwa.obase.data.table.TFilePath;
import online.nvwa.obase.data.table.Tfile;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Vector;

public class FileStorage {
	public static boolean fileExist(String uri){
		TFilePath fpath = new TFilePath(uri);
		return fpath.needConnect();
	}

	public static byte[] getFile(String uri){
		TFilePath fpath = new TFilePath(uri);
		if(fpath.needConnect())return null;
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Scan scan = new Scan(Bytez.from(fpath.getId()), Bytez.from(fpath.getId()+1));
		scan.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("content"));

		try{
			ResultScanner rs = Obase.getFileTable().getScanner(scan);
			for(Result r: rs){
				baos.write( CellUtil.cloneValue(r.listCells().get(0)));
			}	
			baos.close();
			return baos.toByteArray();
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 分成一系列大小为1MB的文件块 
	 * @param data
	 * @param uri
	 * @param override
	 * @return
	 * @throws Exception
	 */
	public static boolean storeFile(byte[] data, String uri, boolean override) throws Exception {
		System.out.println("cloud: " + uri);
		TFilePath fpath = new TFilePath(uri);
		if(!fpath.needConnect() && !override)
			return false;
		
		//存在
		if(!fpath.needConnect()){
			//如果不允许覆盖,返回失败
			if(!override)return false;
			
			//删除现有的
			List<Delete> deletes = new Vector<Delete>();
			Scan scan = new Scan(Bytez.from(fpath.getId()), Bytez.from(fpath.getId()+1));
			scan.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("createTime"));
			ResultScanner rs = Obase.getFileTable().getScanner(scan);
			for(Result r: rs){
				deletes.add(new Delete(r.getRow()));
			}
			Obase.getFileTable().delete(deletes);
		}else{
			fpath.connect();
		}
		
		int index = 0;		
		int offset = 0;
		while(offset < data.length){
			Tfile tf = new Tfile(Bytes.add(Bytez.from(fpath.getId()), Bytez.from(index++)));
			if(tf.needConnect()){
				tf.setAttribute("content", Bytes.copy(data, offset, Math.min(1024*1024, data.length-offset)));
				offset += 1024*1024;
				tf.connect();
			}
		}
		
		return true;
	}

	/**
	 * 分成一系列大小为1MB的文件块 
	 * @param uri
	 * @return
	 * @throws Exception
	 */
	public static boolean deleteFile(String uri) throws Exception {
		TFilePath fpath = new TFilePath(uri);

		//存在
		if(!fpath.needConnect()){
			//删除现有的
			List<Delete> deletes = new Vector<Delete>();
			Scan scan = new Scan(Bytez.from(fpath.getId()), Bytez.from(fpath.getId()+1));
			scan.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("createTime"));
			ResultScanner rs = Obase.getFileTable().getScanner(scan);
			for(Result r: rs){
				deletes.add(new Delete(r.getRow()));
			}
			Obase.getFileTable().delete(deletes);
			return true;
		}
		return false;
	}
}
