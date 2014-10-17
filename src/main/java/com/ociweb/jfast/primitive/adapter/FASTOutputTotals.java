package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FASTOutputTotals implements FASTOutput {

    private long total;
    private DataTransfer dataTransfer;
    
    public FASTOutputTotals() {
    }

    @Override
    public void init(DataTransfer dataTransfer) {
        this.dataTransfer = dataTransfer;
    }
    
    @Override
    public void flush() {
        int size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
        while (size>0) {
            total+=size;
            PrimitiveWriter.nextOffset(dataTransfer.writer);
            size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);      
        }
    }
    
    /**
     * Total bytes that have been consumed by this sink
     * @return
     */
    public long total() {
        return total;
    }


}
