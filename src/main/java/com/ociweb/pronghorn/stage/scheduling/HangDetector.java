package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HangDetector {

	private final long timeout;
	private final static Logger logger = LoggerFactory.getLogger(HangDetector.class);
	private static AtomicBoolean globalRun = new AtomicBoolean(true);
	
	private String activeName;
	private Thread thread;
	private long activeTimeout;
	
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
					
					if (activeTimeout>0 && activeTimeout<System.nanoTime()) {
						
						logger.error("Hang detected in: {}", activeName);
						globalRun.set(false); //stop all other detectors since they are likely to trigger with false positive.
						break; //stop any more checks..
					}
					try {
						Thread.sleep(20);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
							
				
			}			
		};
	}

	public void begin(String name) {
		this.activeName = name;		
		this.activeTimeout = System.nanoTime()+timeout;
		
	}

	public void finish() {
		this.activeTimeout =  -1;
	}

}
