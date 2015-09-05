package com.ociweb.pronghorn.stage.route;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RoundRobinRouteStage extends PronghornStage {

	RingBuffer inputRing;
	RingBuffer[] outputRings;
	int targetRing;
	int targetRingInit;
	int msgId = -2;
	
	public RoundRobinRouteStage(GraphManager gm, RingBuffer inputRing, RingBuffer ... outputRings) {
		super(gm,inputRing,outputRings);
		this.inputRing = inputRing;
		this.outputRings = outputRings;
		this.targetRingInit = outputRings.length-1;
		this.targetRing = targetRingInit;
		
		this.supportsBatchedPublish = true;
		this.supportsBatchedRelease = true;
		
		//TODO:AAAA must confirm that the output rings are just as big or bigger than inputs and that both have the same schema!!
		
		
	}

	@Override
	public void run() {		
	    
	      do {	          
	            if (-2==this.msgId) {
	                if (RingReader.tryReadFragment(this.inputRing)) {
	                    if ((this.msgId = RingReader.getMsgIdx(this.inputRing))<0) {
	                        this.requestShutdown();
	                        return;
	                    }       
	                } else {	                    
	                    return;
	                }
	            }
	    
	            if (RingReader.tryMoveSingleMessage(this.inputRing, this.outputRings[this.targetRing])) {
	                RingReader.releaseReadLock(this.inputRing);
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
	    
	//    System.out.println("round robin exit balance:"+temp);
	    
		//send the EOF message to all of the targets.
		int i = outputRings.length;
		while (--i>=0) {
			RingBuffer.publishAllBatchedWrites(outputRings[i]);
		}
	}

	
	
}
