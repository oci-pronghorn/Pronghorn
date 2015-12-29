package com.ociweb.pronghorn.stage.test;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class PipeCleanerStage<T extends MessageSchema> extends PronghornStage {

    private Pipe<T> input;
    private long totalSlabCount = 0;
    private long totalBlobCount = 0;
    private long startTime;
    private long duration;
    
    private long tail;
    private int byteTail;
    
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
    public void startup() {
        startTime = System.currentTimeMillis();
        tail = Pipe.tailPosition(input);
        byteTail = Pipe.getBlobRingTailPosition(input);
    }
    
    @Override
    public void run() {
        
        long head = Pipe.headPosition(input);
        long contentRemaining = head-tail;
        if (contentRemaining>0) {
            totalSlabCount += contentRemaining;
            
            int byteHead = Pipe.getBlobRingHeadPosition(input);
            totalBlobCount+=(long) ((byteHead >= byteTail) ? byteHead-byteTail : input.sizeOfBlobRing - (byteTail-byteHead));
            Pipe.publishBlobWorkingTailPosition(input, byteTail = byteHead);
            Pipe.publishWorkingTailPosition(input, tail = head);            
        }        
    }
    
    @Override
    public void shutdown() {
        duration = System.currentTimeMillis()-startTime;
        
        //TODO: may want boolean to turn this off on construction?
        try {
            System.out.println(appendReport(new StringBuilder()));
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    public long totalBytes() {
        return (4*totalSlabCount)+totalBlobCount;
    }
    
    public <A extends Appendable> A appendReport(A target) throws IOException {
        
        Appendables.appendValue(target, "Duration :",duration,"ms\n");
        if (0!=duration) {
            long kbps = (totalBytes()*8L)/duration;
            if (kbps>16000) {
                Appendables.appendValue(target, "mbps :",(kbps/1000),"\n");        
            } else {
                Appendables.appendValue(target, "kbps :",(kbps),"\n");     
            }
        }
        return target;
    }
}
