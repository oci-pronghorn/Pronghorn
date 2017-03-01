package com.ociweb.pronghorn.stage;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;


public abstract class PronghornStage {
	
	private static final Logger logger = LoggerFactory.getLogger(PronghornStage.class);
	public static final Pipe[] NONE = new Pipe[0];
	
	//What if we only have 1 because this is the first or last stage?

	public final int stageId; //since these are unique also used for hash
	private final int hash;
	
	private GraphManager graphManager;	
	protected boolean supportsBatchedRelease = true;
	protected boolean supportsBatchedPublish = true;
	
	@Override
	public int hashCode() {
		return hash;
	}
	
	@Override
	public boolean equals(Object obj) {		
		if (obj instanceof PronghornStage) {
			PronghornStage that = (PronghornStage)obj;
			return that.hash == this.hash;
		} else {
			return false;
		}
	}
	
	private int outgoingNoRoom;
	private int outgoingRoom;
	private int incomingNoContent;
	private int incomingContent;
	
	protected boolean recordOutgoingState(boolean noRoom) {
		if (noRoom) {
			outgoingNoRoom++;
		} else {
			outgoingRoom++;
		}
		return true;
	}

	
	protected boolean recordIncomingState(boolean noContent) {
		if (noContent) {
			incomingNoContent++;
		} else {
			incomingContent++;
		}
		return true;
	}
	
	protected boolean reportRecordedStates(String label) {
		
		int totalOut = outgoingNoRoom+outgoingRoom;
		if (totalOut>0) {
			logger.info("{} outgoing  NoRoom:{}    Room:{}     Total:{}  Blocked:{}%",label, outgoingNoRoom, outgoingRoom, totalOut, 100f*outgoingNoRoom/(outgoingNoRoom+outgoingRoom));
		}
		int totalIn = incomingNoContent+incomingContent;
		if(totalIn>0) {
			logger.info("{} incoming  NoContent:{} Content:{}  Total:{}  Blocked:{}%",label, incomingNoContent, incomingContent, totalIn, 100f*incomingNoContent/(incomingNoContent+incomingContent));
		}
		return true;
	}
	
    /**
     * @return the maximum variable length supported across all the pipes
     */
    protected static int maxVarLength(Pipe<?>[] pipes) {
    	int result = 0;
    	int i = pipes.length;
    	while (--i>=0) {
    		result = Math.max(result, pipes[i].maxAvgVarLen);
    	}
		return result;
	}

    /**
     * @return the minimum variable length supported across all the pipes
     */
	protected static int minVarLength(Pipe<?>[] pipes) {
		int result = Integer.MAX_VALUE;
    	int i = pipes.length;
    	while (--i>=0) {
    		result = Math.min(result, pipes[i].maxAvgVarLen);
    	}
		return Integer.MAX_VALUE==result ? 0 : result;
	}
	
	//in the constructor us a zero length array if there are no values.
	protected PronghornStage(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs) {
	    assert(null!=inputs) : "Use NONE";
	    assert(null!=outputs) : "Use NONE";
	    assert(noContainedNull(inputs)) : "Null disovered in array";
	  //  assert(noContainedNull(outputs)) : "Null disovered in array"; //TODO: put back in and find the bug in IoT project
		
	    this.stageId = GraphManager.newStageId(graphManager);
	    this.hash = PronghornStage.class.hashCode() ^ stageId;
		this.graphManager = graphManager;
		GraphManager.register(graphManager, this, inputs, outputs);
		GraphManager.addNota(graphManager, GraphManager.THREAD_GROUP, null, this); //This provides room for assignment later
	}
	
	protected PronghornStage(GraphManager graphManager, Pipe input, Pipe[] outputs) {
	    assert(null!=input) : "Use NONE";
	    assert(null!=outputs) : "Use NONE";
	    assert(noContainedNull(outputs)) : "Null disovered in array";
		assert(input!=null);
		
	    this.stageId = GraphManager.newStageId(graphManager);
	    this.hash = PronghornStage.class.hashCode() ^ stageId;
		this.graphManager = graphManager;
		GraphManager.register(graphManager, this, input, outputs);
		GraphManager.addNota(graphManager, GraphManager.THREAD_GROUP, null, this);//This provides room for assignment later
	}
    
	protected PronghornStage(GraphManager graphManager, Pipe[] inputs, Pipe output) {
	    assert(null!=inputs) : "Use NONE";
	    assert(null!=output) : "Use NONE";
	    assert(noContainedNull(inputs)) : "Null disovered in array";	
		assert(output!=null);
	    
	    this.stageId = GraphManager.newStageId(graphManager);
	    this.hash = PronghornStage.class.hashCode() ^ stageId;
		this.graphManager = graphManager;
		GraphManager.register(graphManager, this, inputs, output);
		GraphManager.addNota(graphManager, GraphManager.THREAD_GROUP, null, this);//This provides room for assignment later
	}
	
	private boolean noContainedNull(Pipe[] inputs) {
		int i = inputs.length;
		while (--i>=0) {
			if (null==inputs[i]) {
				logger.warn("null found at index {} in array of Pipes",i);
				return false;
			}
		}
		return true;
	}

	protected PronghornStage(GraphManager graphManager, Pipe input, Pipe output) {
		assert(input!=null);
		assert(output!=null);
		this.stageId = GraphManager.newStageId(graphManager); 
		this.hash = PronghornStage.class.hashCode() ^ stageId;
		this.graphManager = graphManager;
		GraphManager.register(graphManager, this, input, output);
		GraphManager.addNota(graphManager, GraphManager.THREAD_GROUP, null, this);//This provides room for assignment later
	}

    public static Pipe[] join(Pipe[] ... pipes) {
        
        int totalCount = 0;
        int j = pipes.length;
        while (--j>=0) {
            
            Pipe[] localPipes = pipes[j];
            totalCount += localPipes.length;
            
        }
        
        Pipe[] p = new Pipe[totalCount];
        j = 0;
        
        for(int i = 0; i<pipes.length; i++) {
            for(int k = 0; k<pipes[i].length; k++) {
                p[j++] = pipes[i][k];
            }
        }
        
        return p;
    }
    

    
   public static Pipe[] join(Pipe[][][] pipes) {
        
        int j = pipes.length;
        Pipe[][] p = new Pipe[j][];
        while (--j>=0) {        	
        	Pipe[][] temp = pipes[j];        	        	
            p[j] = join(temp);           
            
        }        
        return join(p);
    }
    
    
     public static Pipe[] join(Pipe[] pipes, Pipe ... additional) {
        
    	if (null==additional) {
    		return pipes;
    	}
    	
        int totalCount = pipes.length+additional.length;
        
        Pipe[] p = new Pipe[totalCount];
        System.arraycopy(pipes, 0, p, 0, pipes.length);
        System.arraycopy(additional, 0, p, pipes.length, additional.length);
        
        return p;
    }
    
    public static Pipe[] join(Pipe ... pipes) {
        return pipes;
    }
    
	public void reset() {
	    //TODO: build new error recovery into scheduler
	    //      after exception position tail to re-read the same block an try again N times.
	    //      after the N attempts skip over that block and continue
	    //      after exception is caught and position is modified reset() is called before next run() call.
	    
	    
	}
	
	
	public void startup() {
		
		//override to do work that must be done once.
		//    Allocation of Objects/Memory so that it is done on this thread and supports Numa
		//    Database connections
		//    Other one time setup work.

	}

	public String toString() {
        return Appendables.appendValue(new StringBuilder().append(getClass().getSimpleName()), " #", stageId).toString();
	}
	
	public void shutdown() {
		
		//stages need to write their own shutdown but never call it.
		
	}

	public void requestShutdown() {
		
		if (!GraphManager.isStageShuttingDown(graphManager, stageId)) {
			GraphManager.setStateToStopping(graphManager, stageId);
		}

	}

	/**
	 * Process all the work that is immediately available.
	 * Should periodically return, if it is away too long this can be controlled by making the output ring smaller.
	 * 
	 */
    public abstract void run();

    public static boolean supportsBatchedRelease(PronghornStage stage) {
		return stage.supportsBatchedRelease;
	}
	
    public static boolean supportsBatchedPublish(PronghornStage stage) {
		return stage.supportsBatchedPublish;
	}
	
}
