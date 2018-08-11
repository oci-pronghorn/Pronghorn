package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Takes an array and an input pipe.
 * If the bytes match the raw bytes, wasEqual will be true.
 * For testing RawDataSchema.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ByteArrayEqualsStage extends PronghornStage {

    private Pipe<RawDataSchema> input;
    private byte[] expected;
    private int expectedPos;
    private static final int SIZE = RawDataSchema.FROM.fragDataSize[0];
    boolean isEqual;

    /**
     *
     * @param gm
     * @param input _in_ Pipe with RawDataSchema that will compares to the byte array
     * @param expected
     */
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
                Pipe.releaseReadLock(input);
                Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
                requestShutdown();
                return;
            }
            int meta = Pipe.takeByteArrayMetaData(input); //for string and byte array
            int len = Pipe.takeByteArrayLength(input);
                            
            if (len < 0) {
                Pipe.releaseReadLock(input);
                Pipe.confirmLowLevelRead(input, SIZE);
                requestShutdown();
                return;
            }
            
            //a single false will make this false
            isEqual &= Pipe.isEqual(input, expected, expectedPos, meta, len);
            expectedPos += len;
            
            Pipe.confirmLowLevelRead(input, SIZE);
            Pipe.releaseReadLock(input);
        }
        
        
    }

    
    public boolean wasEqual() {
        return isEqual && (expectedPos==expected.length);
    }
    

}
