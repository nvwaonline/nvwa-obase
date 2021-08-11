package online.nvwa.obase.data;

import online.nvwa.obase.utils.Obase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Implements the scanner interface for the HBase client.
 */
public class TurboScanner extends AbstractClientScanner {
    private final Log LOG = LogFactory.getLog(this.getClass());

    // special marker to indicate when a scanning task has finished
    private static final Result MARKER = new Result();
    // the size limited buffer to use
    private BlockingDeque<Result> results;

    private volatile IOException exception;

    private Thread thread ;
    private ResultScanner rs;
    private boolean scanFinished = false;

    /**
     * Create a new push scanner.
     *
     * @param scan A Scan object describing the scan
     * @throws IOException
     */
    public TurboScanner(Class entityClass, Scan scan, int bufferSize) throws IOException {
        results = new LinkedBlockingDeque<Result>(bufferSize);

        this.thread = new Thread(){
            public void run(){
//                LOG.debug("Starting " + Bytes.toStringBinary(scan.getStartRow()));
                try {
                    TurboScanner.this.rs = Obase.getTable(entityClass).getScanner(scan);
                    for(Result r: rs){
                        results.put(r);
                    }
                    results.put(MARKER);
                }catch(Exception x){
                    TurboScanner.this.exception = x instanceof IOException ? (IOException) x : new IOException(x);
                }
//                LOG.debug("Finished " + Bytes.toStringBinary(scan.getStopRow()));
            }
        };
        this.thread.start();
    }

    @Override
    public Result next() throws IOException {
        try {
            // if at least one task is active wait for results to arrive.
            if (exception != null) {
                throw exception;
            }

            if(!scanFinished){
                Result r = results.take();
                // skip markers, adjust task count if needed
                if (r == MARKER) {
                    scanFinished = true;
                }else {
                    return r;
                }
            }

            return results.poll();
        } catch (InterruptedException x) {
            Thread.currentThread().interrupt();
            throw new IOException(x);
        }
    }

    @Override
    public void close() {
        results = null;
        if(thread.isAlive()) {
            thread.interrupt();
        }
        this.rs.close();
    }

    @Override
    public boolean renewLease(){
        return true;
    }
}
