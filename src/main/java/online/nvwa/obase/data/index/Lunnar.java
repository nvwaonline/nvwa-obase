package online.nvwa.obase.data.index;

import java.util.concurrent.*;

public class Lunnar {	
	private  ExecutorService executer = null;
	private  LinkedBlockingQueue<Runnable> queue = null;
	
	public Lunnar(){
		this(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors()*5);
	}
	public Lunnar(int size){ this(size, 1000);	}
	public Lunnar(int size, int queueSize){
		if(size > 500)size = 500;
		if(size<1)size = 1;
		if(queueSize < size)queueSize = size;

		queue = new LinkedBlockingQueue<Runnable>(queueSize);
		executer = new ThreadPoolExecutor(size, size,
				0L, TimeUnit.MILLISECONDS,
				queue,
				(r, executor) -> {
					try {
						executor.getQueue().put(r);
					} catch (InterruptedException e) {
						throw new RejectedExecutionException("interrupted", e);
					}
				});
	}


	public int getBlockingSize(){
		return queue.size();
	}
	
	public void submit(Runnable job){
		executer.submit(job);
	}

	/**
	 * 默认调用，最多等待10分钟
	 */
	public void waitForComplete(){
		executer.shutdown();
		
		try {//等待直到所有任务完成
			executer.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	public void waitForComplete(long time, TimeUnit timeUnit){
		executer.shutdown();

		try {//等待直到所有任务完成
			executer.awaitTermination(time, timeUnit);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void close(){
		if(!executer.isShutdown()) {
			executer.shutdownNow();
		}
	}
}
