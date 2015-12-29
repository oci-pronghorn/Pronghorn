package com.ociweb.pronghorn.stage.phast;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class LongDataGenStage extends PronghornStage{

    private Pipe<PhastCodecSchema>[] outputPipe;
    private int iterCountDown;
    private long[] head;
    private int pipeIdx = 0;
    private int chunkCountDown;
    private final int chunkSize;
    
    public LongDataGenStage(GraphManager gm, Pipe<PhastCodecSchema>[] outputPipe, int iterations, int chunkSize) {
        super(gm, NONE, outputPipe);  
        this.outputPipe = outputPipe;
        this.iterCountDown = iterations;
        GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
        
        this.chunkSize = chunkSize;
        this.chunkCountDown = chunkSize;
    }
    
    @Override
    public void startup() {
        
        int i = outputPipe.length;
        this.head = new long[i];
        while (--i>=0) {
            this.head[i] = Pipe.headPosition(outputPipe[i]);
        }
        
    }

    @Override
    public void run() {
        
        do {
            if (chunkCountDown<0) {
                if (++pipeIdx >= outputPipe.length) {
                    pipeIdx = 0;
                }
                chunkCountDown = chunkSize;
            }
            
            long filled = head[pipeIdx]-Pipe.tailPosition(outputPipe[pipeIdx]);
            int remaining = (int)(outputPipe[pipeIdx].sizeOfSlabRing-filled);
            if (remaining>=62) {
                
                int[] slab = Pipe.slab(outputPipe[pipeIdx]);
                int mask = Pipe.slabMask(outputPipe[pipeIdx]);
                
                int pos = (int)head[pipeIdx];
                while ((remaining-=62)>=0 && (--chunkCountDown>=0) && (--iterCountDown >= 0) ) {
                                   
                    
                    slab[mask & pos++] = PhastCodecSchema.MSG_030_10030;
                    int j = 30;
                    while (--j>=0) {
                        
                        long value = (((long)j)<<13);
                        
                        slab[mask & pos++] = (int)(value >>> 32);
                        slab[mask & pos++] = ((int)value);
                        
                    }
                    
                    slab[mask & pos++] = 0;
                    
                    head[pipeIdx]+= 62;            
                
                }
                Pipe.publishWorkingHeadPosition(outputPipe[pipeIdx],  head[pipeIdx]);
    
                
                if (iterCountDown<0) {                
                    requestShutdown();
                    return;
                }
            }
        } while(chunkCountDown<0);
        
    }
    


}
