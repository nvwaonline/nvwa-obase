package online.nvwa.obase.observer;

import online.nvwa.obase.utils.Obase;
import online.nvwa.obase.utils.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;

public abstract class RowExecObserver implements RegionObserver{
	public static final Log LOG = LogFactory.getLog(RowExecObserver.class);

	public Optional<RegionObserver> getRegionObserver(){	
		return Optional.of(this);
	}


	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)throws IOException {
//		e.getEnvironment().getRegion().getScanner(new Scan());


		NavigableSet<byte[]> set =  get.getFamilyMap().get(Bytes.toBytes(Obase.FAMILY_EXEC));

		if(set!=null && !set.isEmpty()){
			byte[] ret = null;
			try {
				Object obj = getTableObject(get.getRow());
				if(obj != null)
					ret = Util.invoke(obj, set.first());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			if(ret != null){
				Cell cell = new KeyValue(get.getRow(), Bytes.toBytes(Obase.FAMILY_EXEC), 
						Bytes.toBytes("ret"), ret);
				results.add(cell);
			}

			e.bypass();
			//e.complete();
		}
	}	


	public abstract Object getTableObject(byte[] row);
}
