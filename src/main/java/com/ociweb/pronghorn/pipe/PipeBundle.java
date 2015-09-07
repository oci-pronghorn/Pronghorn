package com.ociweb.pronghorn.pipe;

public class PipeBundle {
    
    public final Pipe[] buffers;
    private Pipe[] uniqueBuffers;
    
    
    public PipeBundle(Pipe[] buffers) {
        this.buffers = buffers;
        //Many of these buffers are duplicates because they are used by multiple script indexes.
        //Consolidate all the buffers into a single array
        
        //count unique buffers
        int count = 0;
        int i = buffers.length;
        while (--i>=0) {            
            int j = i;
            while (--j>=0) {
                if (buffers[i]==buffers[j]) {
                    break;
                }
            }
            count += (j>>>31); //add the high bit because this is negative when we reach the end             
        }              
        
        this.uniqueBuffers = new Pipe[count];
        count = 0;
        i = buffers.length;
        while (--i>=0) {            
            int j = i;
            while (--j>=0) {
                if (buffers[i]==buffers[j]) {
                    break;
                }
            }
            uniqueBuffers[count] = buffers[i];
            count += (j>>>31); //add the high bit because this is negative when we reach the end             
        }        
                
    }

    public static PipeBundle buildRingBuffers(Pipe rb) {
        FieldReferenceOffsetManager from = Pipe.from(rb);
    	int scriptLength = 0==from.tokens.length ? 1 : from.tokens.length;
		
    	Pipe[] buffers = new Pipe[scriptLength];
		int i = scriptLength;
	    while (--i>=0) {
	        buffers[i]=rb;            
	    }        
	    return new PipeBundle(buffers);
    }

	public static void reset(PipeBundle ringBuffers) {
        //reset all ringbuffers
        int j = ringBuffers.buffers.length;
        while (--j>=0) {
            ringBuffers.buffers[j].reset();
        }
    }

    public static FieldReferenceOffsetManager getFrom(PipeBundle ringBuffers) {
        return Pipe.from(ringBuffers.buffers[0]); //NOTE: all ring buffers have the same instance
    }
    
    public static Pipe get(PipeBundle ringBuffers, int idx) {
        return ringBuffers.buffers[idx];
    }
    
    public static Pipe[] buffers(PipeBundle ringBuffers) {
        return ringBuffers.uniqueBuffers;
    }
    
}
