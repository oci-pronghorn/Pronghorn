package com.ociweb.pronghorn.stage.test;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class PipeCleanerStage<T extends MessageSchema<T>> extends PronghornStage {

    private long totalSlabCount = 0;
    private long totalBlobCount = 0;
    private long startTime;
    private long duration;
    private final String label;
    
    //TODO: make 3 arrays to be dumped
    private Pipe<T>[] input;
    private long[] tail;
    private int[] byteTail;
    
    public static final boolean showVolumeReport = false;
    
    private int pos = 0;
    
    public static PipeCleanerStage newInstance(GraphManager gm, Pipe pipe) {
        return new PipeCleanerStage(gm, pipe);
    }

    public static PipeCleanerStage newInstance(GraphManager gm, Pipe[] pipes) {
        return new PipeCleanerStage(gm, pipes, "");
    }

    public static PipeCleanerStage newInstance(GraphManager gm, Pipe pipe, String label) {
        return new PipeCleanerStage(gm, pipe, label);
    }

    public static PipeCleanerStage newInstance(GraphManager gm, Pipe[] pipes, String label) {
        return new PipeCleanerStage(gm, pipes, label);
    }
    
    //NOTE: this should be extended to produce a diagnostic stage 
    public PipeCleanerStage(GraphManager graphManager, Pipe<T> input) { 
    	this(graphManager, input, "");
    }
    
    public PipeCleanerStage(GraphManager graphManager, Pipe<T> input, String label) {
        this(graphManager, new Pipe[]{input},label);
    }

    public PipeCleanerStage(GraphManager graphManager, Pipe<T>[] input, String label) { 
    	super(graphManager, input, NONE);
    	this.input = input;
        this.label = label;
        this.tail = new long[input.length];
        this.byteTail = new int[input.length];
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
        tail[pos] = Pipe.tailPosition(input[pos]);
        byteTail[pos] = Pipe.getBlobRingTailPosition(input[pos]);
    }
    
    @Override
    public void run() {
    	
    	int i = input.length;
    	while (--i >= 0) {
    		cleanPipe(i);
    	}
        
    }

	private void cleanPipe(int sourcePos) {
		long head = Pipe.headPosition(this.input[sourcePos]);
        long contentRemaining = head-tail[sourcePos];
        if (contentRemaining>0) {
        	
        	totalSlabCount += contentRemaining;
            
            int byteHead = Pipe.getBlobRingHeadPosition(this.input[sourcePos]);
            
            if (byteHead >= byteTail[sourcePos]) {
                totalBlobCount += byteHead-byteTail[sourcePos];
            } else {
                totalBlobCount += (long) (Pipe.blobMask(this.input[sourcePos])&byteHead);                
                totalBlobCount += (long)(this.input[sourcePos].sizeOfBlobRing-(Pipe.blobMask(this.input[sourcePos])&byteTail[sourcePos]));
            } 
            
            Pipe.publishBlobWorkingTailPosition(this.input[sourcePos], byteTail[sourcePos] = byteHead);
            Pipe.publishWorkingTailPosition(this.input[sourcePos], tail[sourcePos] = head);            
            
        } else {
        	
        	if (Pipe.isEndOfPipe(this.input[sourcePos], tail[sourcePos]) && Pipe.contentRemaining(this.input[sourcePos])==0) {
        		requestShutdown();
        	}
        }
	}
    
    @Override
    public void shutdown() {
        duration = System.currentTimeMillis()-startTime;
        
        //TODO: loop over all the pipes and confirm this inside an assert METHOD.
        //assert(Pipe.contentRemaining(input[pos])==0) : "expected pipe to be empty but found "+input;
        

        if (showVolumeReport) {
	        try {
	            System.out.println(appendReport(new StringBuilder()));
	        } catch (IOException e) {
	           throw new RuntimeException(e);
	        }
        }
    }

    public long totalBytes() {
        return (4L*totalSlabCount)+totalBlobCount;
    }
    
    public <A extends Appendable> A appendReport(A target) throws IOException {
        
    	
    	
    	target.append(label);
    	if (label.length()>0) {
    		target.append(' ');
    	}

        Appendables.appendValue(target, "Duration :",duration,"ms  ");
       // Appendables.appendValue(target, "BlobOnlyCount :",totalBlobCount,"\n");        
        Appendables.appendValue(target, "TotalBytes :",totalBytes(),"  ");
        Appendables.appendValue(target, "BlobCount :",totalBlobCount,"  ");
                
        if (0!=duration) {
            long kbps = (totalBytes()*8L)/duration;
            if (kbps>16000) {
                Appendables.appendValue(target, "mbps :",(kbps/1000L),"\n");        
            } else {
                Appendables.appendValue(target, "kbps :",(kbps),"\n");     
            }
        } else {
        	target.append("\n");
        }
        return target;
    }

}
