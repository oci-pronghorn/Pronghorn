package com.ociweb.pronghorn.stage.route;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RoundRobinRouteStage<T extends MessageSchema<T>> extends PronghornStage {

	private Pipe<T> inputRing;
	private Pipe<T>[] outputRings;
	private int targetRing;
	private int targetRingInit;
	private int msgId = -2;
	
	public RoundRobinRouteStage(GraphManager gm, Pipe<T> input, Pipe<T> ... outputs) {
		super(gm,input,outputs);
		this.inputRing = input;
		this.outputRings = outputs;
		this.targetRingInit = outputs.length-1;
		this.targetRing = targetRingInit;
		
		this.supportsBatchedPublish = true;
		this.supportsBatchedRelease = true;
	
		assert(validateTargetSize(input, outputs)) : "output pipes must be as large or larger than input";
	}

    private boolean validateTargetSize(Pipe<T> inputRing, Pipe<T>... outputRings) {
        boolean ok = true;
		PipeConfig<T> sourceConfig = inputRing.config();
		int i = outputRings.length;
		while (--i >= 0) {
		    Pipe<T> pipe = outputRings[i];		    
		    PipeConfig<T> targetConfig = pipe.config();		    
		    ok = ok & targetConfig.canConsume(sourceConfig);		    
		}
        return ok;
    }

	@Override
	public void run() {		
	    
	      do {	          
	            if (-2==this.msgId) {
	                if (PipeReader.tryReadFragment(this.inputRing)) {
	                    //shutdown logic
	                    if ((this.msgId = PipeReader.getMsgIdx(this.inputRing))<0) {
	                        PipeReader.releaseReadLock(this.inputRing);
	                        this.requestShutdown();
	                        return;
	                    }       
	                } else {	        
	                    //nothing to read so try again
	                    return;
	                }
	            }
	    
	            //continue trying to move this message until we reach the end
	            if (PipeReader.tryMoveSingleMessage(this.inputRing, this.outputRings[this.targetRing])) {
	                //we have reached the end of the message
	                PipeReader.releaseReadLock(this.inputRing);
	                if (--this.targetRing<0) {
	                    this.targetRing = this.targetRingInit;
	                }               
	                this.msgId = -2;
	            } else {
	                return;
	            }

	        } while(true);  
	}

	//TODO: AA, May need NEW round robin stage that is low level and only works with messages
	//TODO: AA, May also need to add cursor to head of all fragments.


	@Override
	public void shutdown() {
		//send the EOF message to all of the targets.
		int i = outputRings.length;
		while (--i>=0) {
			Pipe.publishAllBatchedWrites(outputRings[i]);
		}
	}

	
	
}
