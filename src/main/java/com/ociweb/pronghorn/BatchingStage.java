package com.ociweb.pronghorn;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class BatchingStage<T extends MessageSchema<T>> extends PronghornStage {

	private final int limit;
	private final Pipe<T> input;
	private final Pipe<T> output;
	
	public BatchingStage(GraphManager gm, double pct, Pipe<T> input, Pipe<T> output) {
		super(gm, input, output);
	    
		//when we hit this limit we can then send all the data out
		this.limit = (int)(pct * input.sizeOfSlabRing);
	
		this.input = input;
		this.output = output;
		
	}

	@Override
	public void run() {
		
		//only move the data if we are over the lmit
		int remaining;
		if ((remaining = Pipe.contentRemaining(input)) >= limit) {
			
			//move all the data we can
			while ( remaining > 0 //active batch still has data
			     && Pipe.hasContentToRead(input) //has a full fragment
				 && Pipe.hasRoomForWrite(output)) {
				
				int size = Pipe.copyFragment(input, output);
				
				remaining -= size;	
				
			}
		}
	}

}
