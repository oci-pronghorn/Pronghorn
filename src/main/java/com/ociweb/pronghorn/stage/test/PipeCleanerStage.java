package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PipeCleanerStage<T extends MessageSchema> extends PronghornStage {

    private Pipe<T> input;
    private long totalSlabCount = 0;
    private long totalBlobCount = 0;
    
    
    //NOTE: this should be extended to produce a diagnostic stage 
    
    public PipeCleanerStage(GraphManager graphManager, Pipe<T> input) {
        super(graphManager, input, NONE);
        this.input = input;
    }

    public long getTotalSlabCount() {
        return totalSlabCount;
    }
    
    public long getTotalBlobCount() {
        return totalBlobCount;
    }
    
    
    @Override
    public void run() {
        
        long head = Pipe.headPosition(input);
        long tail = Pipe.tailPosition(input);
        long contentRemaining = head-tail;
        if (contentRemaining>0) {
            totalSlabCount += contentRemaining;
            
            int byteHead = Pipe.getBlobRingHeadPosition(input);
            int byteTail = Pipe.getBlobRingTailPosition(input);
            long dif;
            if (byteHead >= byteTail) {
                dif = byteHead-byteTail;
            } else {
                dif = input.sizeOfBlobRing - (byteTail-byteHead);
            }
            totalBlobCount+=dif;
            
            Pipe.publishBlobWorkingTailPosition(input, byteHead);
            Pipe.publishWorkingTailPosition(input, head);
            
        }        
    }

    public long totalBytes() {
        return (4*totalSlabCount)+totalBlobCount;
    }
}
