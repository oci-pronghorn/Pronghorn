package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ByteArrayEqualsStage extends PronghornStage {

    private Pipe<RawDataSchema> input;
    private byte[] expected;
    private int expectedPos;
    private static final int SIZE = RawDataSchema.FROM.fragDataSize[0];
    boolean isEqual;
    
    public ByteArrayEqualsStage(GraphManager gm, Pipe<RawDataSchema> input, byte[] expected) {
        super(gm, input, NONE);
        this.input = input;
        this.expected = expected;
        this.expectedPos = 0;
        this.isEqual = true;
    }

    @Override
    public void run() {
        
        while (Pipe.hasContentToRead(input)) {
            
            int msgId = Pipe.takeMsgIdx(input);   
            if (msgId < 0) {
                Pipe.releaseReads(input);
                Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
                requestShutdown();
                return;
            }
            int meta = Pipe.takeRingByteMetaData(input); //for string and byte array
            int len = Pipe.takeRingByteLen(input);
                            
            if (len < 0) {
                Pipe.releaseReads(input);
                Pipe.confirmLowLevelRead(input, SIZE);
                requestShutdown();
                return;
            }
            
            //a single false will make this false
            isEqual &= Pipe.isEqual(input, expected, expectedPos, meta, len);
            expectedPos += len;
            
            Pipe.confirmLowLevelRead(input, SIZE);
            Pipe.releaseReads(input);
        }
        
        
    }

    
    public boolean wasEqual() {
        return isEqual && (expectedPos==expected.length);
    }
    

}
