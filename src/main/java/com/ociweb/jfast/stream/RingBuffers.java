package com.ociweb.jfast.stream;

import com.ociweb.jfast.ring.RingBuffer;
import com.ociweb.jfast.ring.FieldReferenceOffsetManager;

public class RingBuffers {
    
    public final RingBuffer[] buffers;
    private RingBuffer[] uniqueBuffers;
    
    
    public RingBuffers(RingBuffer[] buffers) {
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
            count += (j>>>31); //add the high bit because this is negative              
        }
        //System.err.println("unique buffers:"+count);
              
        
        this.uniqueBuffers = new RingBuffer[count];
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
            count += (j>>>31); //add the high bit because this is negative              
        }        
                
    }
    
    //package protected
    RingBuffer[] rawArray() {
        return buffers;
    }

    public static void reset(RingBuffers ringBuffers) {
        //reset all ringbuffers
        int j = ringBuffers.buffers.length;
        while (--j>=0) {
            ringBuffers.buffers[j].reset();
        }
    }

    public static FieldReferenceOffsetManager getFrom(RingBuffers ringBuffers) {
        return ringBuffers.buffers[0].consumerData.from; //NOTE: all ring buffers have the same instance
    }
    
    public static RingBuffer get(RingBuffers ringBuffers, int idx) {
        return ringBuffers.buffers[idx];
    }
    
    public static RingBuffer[] buffers(RingBuffers ringBuffers) {
        return ringBuffers.uniqueBuffers;
    }
    
}
