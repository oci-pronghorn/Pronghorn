package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StageScheduler {

	static final Logger log = LoggerFactory.getLogger(StageScheduler.class);
	protected GraphManager graphManager;
	
	
	public StageScheduler(GraphManager graphManager) {
		GraphManager.disableMutation(graphManager);
		this.graphManager = graphManager;
	}
	
	
	protected boolean validShutdownState() {
		return GraphManager.validShutdown(graphManager);	
	}

	public abstract void startup();
	public abstract void shutdown();
	public abstract boolean awaitTermination(long timeout, TimeUnit unit);
	public abstract boolean TerminateNow();
	
	
	
}
