package com.ociweb.pronghorn.ring.stage;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.GraphManager;
import com.ociweb.pronghorn.ring.RingBuffer;


public abstract class PronghornStage {
	
	private static final Logger log = LoggerFactory.getLogger(PronghornStage.class);
	protected static final RingBuffer[] NONE = new RingBuffer[0];
	
	//What if we only have 1 because this is the first or last stage?

	public final int stageId;	
	public final boolean stateless;
	private static AtomicInteger stageCounter = new AtomicInteger();
	
	private GraphManager pm;
	
	//in the constructor us a zero length array if there are no values.
	protected PronghornStage(GraphManager pm, RingBuffer[] inputs, RingBuffer[] outputs) {
		this.stageId = stageCounter.getAndIncrement();		
		this.pm = pm;
		this.stateless = false;
		GraphManager.register(pm, this, inputs, outputs);
	}
	
	protected PronghornStage(GraphManager pm, RingBuffer input, RingBuffer[] outputs) {
		this.stageId = stageCounter.getAndIncrement();	
		this.pm = pm;
		this.stateless = false;
		GraphManager.register(pm, this, input, outputs);
	}
    
	protected PronghornStage(GraphManager pm, RingBuffer[] inputs, RingBuffer output) {
		this.stageId = stageCounter.getAndIncrement();		
		this.pm = pm;
		this.stateless = false;
		GraphManager.register(pm, this, inputs, output);
	}
	
	protected PronghornStage(GraphManager pm, RingBuffer input, RingBuffer output) {
		this.stageId = stageCounter.getAndIncrement();	
		this.pm = pm;
		this.stateless = false;
		GraphManager.register(pm, this, input, output);
	}
	
	/**
	 * A 'stateless' stage is not aware of the messages or the passing of a posion pill.  
	 * 
	 * @param pm
	 * @param inputs
	 * @param outputs
	 * @param stateless
	 */
	protected PronghornStage(GraphManager pm, RingBuffer[] inputs, RingBuffer[] outputs, boolean stateless) {
		this.stageId = stageCounter.getAndIncrement();		
		this.pm = pm;
		this.stateless = stateless;
		GraphManager.register(pm, this, inputs, outputs);
	}
	
	protected PronghornStage(GraphManager pm, RingBuffer input, RingBuffer[] outputs, boolean stateless) {
		this.stageId = stageCounter.getAndIncrement();	
		this.pm = pm;
		this.stateless = stateless;
		GraphManager.register(pm, this, input, outputs);
	}
    
	protected PronghornStage(GraphManager pm, RingBuffer[] inputs, RingBuffer output, boolean stateless) {
		this.stageId = stageCounter.getAndIncrement();		
		this.pm = pm;
		this.stateless = stateless;
		GraphManager.register(pm, this, inputs, output);
	}
	
	protected PronghornStage(GraphManager pm, RingBuffer input, RingBuffer output, boolean stateless) {
		this.stageId = stageCounter.getAndIncrement();	
		this.pm = pm;
		this.stateless = stateless;
		GraphManager.register(pm, this, input, output);
	}
	public String toString() {
		return getClass().getSimpleName()+"["+String.valueOf(stageId)+"]";
	}
	
    public abstract boolean exhaustedPoll();
	
	
}
