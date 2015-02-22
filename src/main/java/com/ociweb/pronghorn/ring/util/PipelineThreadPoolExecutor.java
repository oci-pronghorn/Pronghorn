package com.ociweb.pronghorn.ring.util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PipelineThreadPoolExecutor extends ThreadPoolExecutor {

	//TODO: lookup of the LOC must go up the tree in case the schema changed.
	//TODO: move executor into the RingBuffers
	//TODO: run must not block and must return.
	
	
	//Instead of runnable we could build our own interface? or base class?
	//the run must return one state,   exit, noWork, didWorkDoneAll, didWorkLeftData
	
	//or could extend stage base class to do all the catch and reporting, build in,   
	
	public PipelineThreadPoolExecutor(int nThreads) {
		super(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
	}

	//reschedule this runable to run again in the near future.
	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		
         if (t == null && r instanceof Future<?>) {
                try {
                  Object result = ((Future<?>) r).get();
                } catch (CancellationException ce) {
                    t = ce;
                } catch (ExecutionException ee) {
                    t = ee.getCause();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // ignore/reset
                }
         } else if (t != null) {
                System.out.println(t);
         } else {
        	    execute(r);
        }
         
	}
	
	

}
