package com.ociweb.pronghorn.stage.scheduling;

import com.ociweb.pronghorn.pipe.PipePublishListener;

public class DidWorkMonitor extends PipePublishListener {

	private boolean didWork;
	private long beginNS;
	private Thread runningThread;
	
	public static boolean didWork(DidWorkMonitor that) {
		//note has side effect of clearing the timer
		that.beginNS=0;
		that.runningThread = null;
		return that.didWork;
	}
	
	
	//TODO: all the did work monitors are also to be monitored to find if any 
	//      threads have blocked and not returned.  if so break them and capture the stack trace
	//////
	
	
	public static void begin(DidWorkMonitor that, long nowNS) {
		that.didWork = false;
		that.beginNS = nowNS;
		that.runningThread = Thread.currentThread();
	}

	@Override
	public void published() {
		didWork = true;
	}

	public boolean isOverTimeout(long now, long timeoutNS) {
		//do not allow change of value while we check
		final long local = beginNS;
		return (local==0) ? false : local+timeoutNS>now;		
	}

	public void interrupt() {
		final Thread local = runningThread;
		if (null != local) {
			local.interrupt();
		}
	}

}
