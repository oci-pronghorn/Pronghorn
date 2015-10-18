package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ByteArrayProducerStage extends PronghornStage{

    private final byte[] rawData;
    private final int rawDataLength;
    private int pos;
    private final int chunkSize;
    private final Pipe<RawDataSchema> output;
    
    public ByteArrayProducerStage(GraphManager gm, byte[] rawData, Pipe<RawDataSchema> output) {
        super(gm, NONE, output);
        this.rawData = rawData;
        this.rawDataLength = rawData.length;
        this.pos = 0;
        this.chunkSize =output.maxAvgVarLen;
        this.output = output;
        GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER,  this);
    }

    @Override
    public void run() {        
        
        while (pos<rawDataLength && Pipe.hasRoomForWrite(output)) {
                        
            int length = Math.min(chunkSize, rawDataLength-pos);
            assert(length>0);
            
            int size = Pipe.addMsgIdx(output, 0);
            Pipe.addByteArray(rawData, pos, length, output);
            Pipe.publishWrites(output);
            Pipe.confirmLowLevelWrite(output, size);
            
            pos+=length;
            
        }
        
        if (pos==rawData.length) {
            Pipe.publishAllBatchedWrites(output);
            requestShutdown();
        }
    }
    
   
    @Override
    public void shutdown() {
        Pipe.spinBlockForRoom(output, Pipe.EOF_SIZE);
        Pipe.publishEOF(output);
        
    }
    
}
