package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ByteArrayProducerStage extends PronghornStage{

    private final byte[] rawData;
    private final int rawDataLength;
    private final int[] optionalChunkSizes;
    private int chunkCount;
    private int pos;
    private final int chunkSize;
    private final Pipe<RawDataSchema> output;
    
    public ByteArrayProducerStage(GraphManager gm, byte[] rawData, Pipe<RawDataSchema> output) {
        this(gm, rawData, null, output);
    }
    
    public ByteArrayProducerStage(GraphManager gm, byte[] rawData, int[] optionalChunkSizes, Pipe<RawDataSchema> output) {
        super(gm, NONE, output);
        this.rawData = rawData;
        this.rawDataLength = rawData.length;
        this.optionalChunkSizes = optionalChunkSizes;
        this.pos = 0;
        this.chunkSize =output.maxVarLen;
        this.output = (Pipe<RawDataSchema>)output;
        GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER,  this);
    }

    @Override
    public void run() {        
        
        while (pos<rawDataLength && Pipe.hasRoomForWrite(output)) {
                        
            int length = computeLength();
                        
            assert(length>0);
            
            int size = Pipe.addMsgIdx(output, 0);
            Pipe.addByteArray(rawData, pos, length, output);
            Pipe.confirmLowLevelWrite(output, size);
            Pipe.publishWrites(output);
            
            pos+=length;
            ++chunkCount;
        }
        
        if (pos==rawData.length) {
            Pipe.publishAllBatchedWrites(output);
            requestShutdown();
        }
    }

    private int computeLength() {
        int length = Math.min(chunkSize, rawDataLength-pos);
        
        if (null != optionalChunkSizes) {
            int defLen = optionalChunkSizes[chunkCount];
            if (defLen>length) {
                throw new UnsupportedOperationException("defined chunk length must have data and fit within the pipe");
            }
            length=defLen;
        }
        return length;
    }
    
   
    @Override
    public void shutdown() {
        Pipe.spinBlockForRoom(output, Pipe.EOF_SIZE);
        Pipe.publishEOF(output);
        
    }
    
}
