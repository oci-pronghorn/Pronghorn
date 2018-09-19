package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.util.Appendables;

public class HangDetector {

	private final long timeout;
	private final static Logger logger = LoggerFactory.getLogger(HangDetector.class);
	private static AtomicBoolean globalRun = new AtomicBoolean(true);
	
	private Object active;
	private Thread activeThread;
	
	/////////////////////////////
	private Thread thread;
	private long activeTimeout = Long.MAX_VALUE;
	
	
	public HangDetector(long timeout) {
		this.timeout = timeout;
		this.thread = new Thread(runnable());
		this.thread.start();
	}

	private Runnable runnable() {
		return new Runnable() {
			@Override
			public void run() {
				
				while (globalRun.get()) {
					
					if (System.nanoTime()>activeTimeout) {
						
							logger.error("Hang detected in: {} after timeout of "+Appendables.appendNearestTimeUnit(new StringBuilder(), timeout), String.valueOf(active));
							globalRun.set(false); //stop all other detectors since they are likely to trigger with false positive.
							
							Thread t = activeThread;
							if (null!=t) {
								//
								for(StackTraceElement s :t.getStackTrace()) {
									System.out.println(s);
								};
								
							}
							
							
							break; //stop any more checks..
					
					}
					
					try {
						Thread.sleep(20);
					} catch (InterruptedException e) {
						break;
					}
				}
							
				
			}			
		};
	}

	public void begin(Object obj, Thread activeThread) {
		this.active = obj;		
		this.activeThread = activeThread;
		this.activeTimeout = System.nanoTime()+timeout;
		assert(activeTimeout > System.nanoTime());
	}

	public void finish() {
		this.activeThread = null;
		this.activeTimeout =  Long.MAX_VALUE;
		this.active = null;//important to ensure no leak.
	}

}
