package com.ociweb.pronghorn.stage.blocking;

public interface BlockingWorkerProducer  {

	BlockingWorker newWorker();

	String name();
	
	
}
