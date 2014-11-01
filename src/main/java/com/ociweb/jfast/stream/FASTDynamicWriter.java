package com.ociweb.jfast.stream;

import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.ring.RingBuffer;

public class FASTDynamicWriter {

    private final FASTEncoder writerDispatch;
    private final RingBuffer ringBuffer;
    final PrimitiveWriter writer;

    public FASTDynamicWriter(PrimitiveWriter primitiveWriter, RingBuffer ringBuffer, FASTEncoder writerDispatch) {

        this.writerDispatch = writerDispatch;
        this.ringBuffer = ringBuffer;
        this.writer = primitiveWriter;
        
    }

    // non blocking write, returns if there is nothing to do.
    public void write() {
        // write from the the queue/ringBuffer
        // queue will contain one full unit to be processed.
        // Unit: 1 Template/Message or 1 EntryInSequence

        // because writer does not move pointer up until full unit is ready to
        // go we only need to check if data is available, not the size.
        if (RingBuffer.contentRemaining(ringBuffer)>0) {
            
            //TODO: B, must add loop check over ringBuffer, is this an internal or external feature?
            
            //NOTE: multiple writers to one location is NEVER allowed therefore
            //this MUST support reading from multiple locations.
            
            
            writerDispatch.encode(writer, ringBuffer);
            
            
        }
    }

    public void reset(boolean clearData) {

        writerDispatch.activeScriptCursor = 0;
        writerDispatch.activeScriptLimit = 0;

        if (clearData) {
            writerDispatch.dictionaryFactory.reset(writerDispatch.rIntDictionary);
            writerDispatch.dictionaryFactory.reset(writerDispatch.rLongDictionary);
            writerDispatch.dictionaryFactory.reset(writerDispatch.byteHeap);
        }
    }

}
