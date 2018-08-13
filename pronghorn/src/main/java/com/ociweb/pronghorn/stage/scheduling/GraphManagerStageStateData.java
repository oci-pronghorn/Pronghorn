package com.ociweb.pronghorn.stage.scheduling;

public class GraphManagerStageStateData {
	
	final Object lock = new Object();	
	byte[] stageStateArray = new byte[GraphManager.INIT_STAGES];
	
	public final static byte STAGE_NEW = 0;
	public final static byte STAGE_STARTED = 1;
	public final static byte STAGE_STOPPING = 2;
	public final static byte STAGE_TERMINATED = 3;
	
}