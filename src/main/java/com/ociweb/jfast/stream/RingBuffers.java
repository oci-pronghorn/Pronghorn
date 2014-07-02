package com.ociweb.jfast.stream;

public class RingBuffers {
    
    private FASTRingBuffer[] buffers;
    
    public RingBuffers(FASTRingBuffer[] buffers) {
        this.buffers = buffers;
    }
    
    //package protected
    FASTRingBuffer[] rawArray() {
        return buffers;
    }

    public static void reset(RingBuffers ringBuffers) {
        //reset all ringbuffers
        int j = ringBuffers.buffers.length;
        while (--j>=0) {
            ringBuffers.buffers[j].reset();
        }
    }

    public static FASTRingBuffer get(RingBuffers ringBuffers, int idx) {
        return ringBuffers.buffers[idx];
    }
    
}
