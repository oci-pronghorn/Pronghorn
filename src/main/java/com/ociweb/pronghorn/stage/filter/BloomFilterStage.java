package com.ociweb.pronghorn.stage.filter;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.RollingBloomFilter;

public class BloomFilterStage<T extends MessageSchema> extends PronghornStage {

    private final Pipe<T> input;
    private final Pipe<T>[] outputs;
    private RollingBloomFilter   bloomFilter;
    private final long    maxElementCount;
    private final double  probability;
    private int           msgId = -2;
    private Pipe<T>       targetPipe;
    
    protected BloomFilterStage(GraphManager graphManager, Pipe<T> input, Pipe<T>[] outputs) {
        super(graphManager, input, outputs);
        this.input = input;
        this.outputs = outputs;
        this.targetPipe = outputs[0];
        
        this.maxElementCount = 1000000;
        this.probability     = 0.0000001d;
    }

    @Override
    public void startup() {
        bloomFilter = new RollingBloomFilter(maxElementCount, probability);
    }
    
    
    @Override
    public void run() {
        int[] slab = Pipe.slab(input);
        byte[] blob = Pipe.blob(input);
        
        int slabMask = Pipe.slabMask(input);
        int blobMask = Pipe.blobMask(input);
        do {            
            if (-2==this.msgId) {
                if (PipeReader.tryReadFragment(this.input)) {
                    //Shutdown logic
                    if ((this.msgId = PipeReader.getMsgIdx(this.input))<0) {
                        PipeReader.releaseReadLock(this.input);
                        this.requestShutdown();
                        return;
                    }  else {
                        //////////////////
                        //NOTE: This bloom filer will only use the content of the first fragment
                        //      as the input to the hash
                        //////////////////
                        
                        int slabLength = Pipe.sizeOf(this.input, this.msgId);
                        long endOfFragment = Pipe.getWorkingTailPosition(input);                        
                        int slabPos = (int)(slabMask & (endOfFragment-slabLength));
                        
                        int blobLength = slab[(int)(slabMask & (endOfFragment-1))];                        
                        int blobPos = Pipe.bytesReadBase(input);
                                                
                        this.targetPipe = outputs[bloomFilter.add(slab,slabPos,slabMask,slabLength,blob,blobPos,blobMask,blobLength)];
                         
                    }
                } else {                        
                    //unable to read anything try again later
                    return;
                }
            }
    
            if (PipeReader.tryMoveSingleMessage(this.input, this.targetPipe)) {
                PipeReader.releaseReadLock(this.input); 
                this.msgId = -2;
            } else {
                return;
            }

        } while(true);
    }
    
    @Override
    public void shutdown() {
        Pipe.publishAllBatchedWrites(outputs[0]);
        Pipe.publishAllBatchedWrites(outputs[1]);
    }

}
