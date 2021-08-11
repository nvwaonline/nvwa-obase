package online.nvwa.obase.data;

import online.nvwa.obase.utils.Obase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Implements the scanner interface for the HBase client.
 * If there are multiple regions in a table, this scanner will iterate
 * through them all in parallel.
 */
public class ParallelScanner extends AbstractClientScanner {
    private final Log LOG = LogFactory.getLog(this.getClass());

    // special marker to indicate when a scanning task has finished
    private static final Result MARKER = new Result();
    // the size limited buffer to use
    private BlockingDeque<Result> results;
    // the thread pool that is used to execute tasks
    private ForkJoinPool pool;
    // number of scanning tasks still running
    private int taskCount;

    private volatile IOException exception;


    /**
     * Create a new parallel scanner.
     *
     * @param connection connection to use for the cluster
     * @param scan       A Scan object describing the scan
     * @param tableName  Name of the table to scan
     * @param threads    maximum number of threads to use
     * @throws IOException
     */
    public ParallelScanner(Class entityClass, Scan scan,
                           int threads) throws IOException {
        this(entityClass, scan, threads, threads * scan.getCaching());
    }

    /**
     * Create a new parallel scanner.
     *
     * @param connection connection to use for the cluster
     * @param scan       A Scan object describing the scan
     * @param tableName  Name of the table to scan
     * @param threads    maximum number of threads to use
     * @param bufferSize maximum number of Result objects to buffer
     * @throws IOException
     */
    public ParallelScanner(Class entityClass, Scan scan,
                           int threads, int bufferSize) throws IOException {
        init(entityClass, scan, bufferSize, threads);
    }

    private void init(Class entityClass, Scan scan,
                      int bufferSize, int threads) throws IOException {
        bufferSize = bufferSize>0?bufferSize:1000;

        results = new LinkedBlockingDeque<Result>(bufferSize);

        List<HRegionLocation> locs = Obase.getRegionsInRange(entityClass, scan.getStartRow(), scan.getStopRow(), false, false);
        LOG.debug("Found "+locs.size()+" regions.");
        if (locs.size() == 0) return;

        taskCount = locs.size();
        List<Scan> scans = new Vector<>();

        // submit the tasks (one per region)
        int i=0;
        for (HRegionLocation loc : locs) {
            Scan s = new Scan(scan);
            s.withStartRow(i==0?scan.getStartRow() : loc.getRegion().getStartKey());
            i++;
            s.withStopRow(i==locs.size()?scan.getStopRow() : loc.getRegion().getEndKey());
            LOG.debug("Submitting "+Bytes.toStringBinary(s.getStartRow()));
            scans.add(s);
        }

        pool = new ForkJoinPool(threads>taskCount?threads:taskCount);
        pool.submit(()->{
            scans.stream().parallel().forEach(e->{
                LOG.debug("Starting " + Bytes.toStringBinary(e.getStartRow()));
                try {
                    ResultScanner rs = Obase.getTable(entityClass).getScanner(e);
                    for(Result r: rs){
                        results.put(r);
                    }
                    results.put(MARKER);
                }catch(Exception x){
                    if (this.exception == null) {
                        this.exception = x instanceof IOException ? (IOException) x : new IOException(x);
                    }
                }
                LOG.debug("Finished " + Bytes.toStringBinary(e.getStartRow()));
            });
        });
    }

    @Override
    public Result next() throws IOException {
        try {
            // if at least one task is active wait for results to arrive.
            Result r;
            while (taskCount > 0 && exception == null) {
                r = results.take();
                // skip markers, adjust task count if needed
                if (r == MARKER) {
                    --taskCount;
                    continue;
                }
                return r;
            }

            if (exception != null) {
                throw exception;
            }

            return results.poll();
        } catch (InterruptedException x) {
            Thread.currentThread().interrupt();
            throw new IOException(x);
        }
    }

    @Override
    public void close() {
        LOG.debug("Scan pool closed.");
        results = null;
        pool.shutdownNow();
        try {
            pool.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException x) {
            // ignore for now
        }
    }

    @Override
    public boolean renewLease(){
        return true;
    }

}
