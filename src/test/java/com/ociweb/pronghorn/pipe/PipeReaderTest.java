package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.util.Appendables;

public class PipeReaderTest {

    
    @Test
    public void powCenters() {
        
        assertEquals(1d,PipeReader.powdi[64],.000001d);
        assertEquals(1f,PipeReader.powfi[64],.00001f);
        
    }

    
    @Test
    public void tryCopyTest() {
        
        PipeConfig<RawDataSchema> configA = new PipeConfig<RawDataSchema>(RawDataSchema.instance,60,256);
        PipeConfig<RawDataSchema> configB = new PipeConfig<RawDataSchema>(RawDataSchema.instance,60,256);
        
        Pipe<RawDataSchema> pipeA = new Pipe<RawDataSchema>(configA);
        Pipe.setPublishBatchSize(pipeA, 0);
        Pipe.setReleaseBatchSize(pipeA, 0);
        pipeA.initBuffers();
        
        Pipe<RawDataSchema> pipeB = new Pipe<RawDataSchema>(configB);
        Pipe.setPublishBatchSize(pipeB, 0);  
        Pipe.setReleaseBatchSize(pipeB, 0);
        pipeB.initBuffers();
        
        int iterations = 5; 
        int batchSize = 47;
        
        //most wrap end twice
        assertTrue(iterations*batchSize*Pipe.sizeOf(pipeA, RawDataSchema.MSG_CHUNKEDSTREAM_1) > pipeA.sizeOfSlabRing*2);
        assertTrue(iterations*batchSize*Pipe.sizeOf(pipeA, RawDataSchema.MSG_CHUNKEDSTREAM_1) > pipeB.sizeOfSlabRing*2);
        
        //repeat the fill, copy, check multiple times to ensure we roll over the end of the pipe
        for(int i=0;i<iterations;i++) {
                
            /////////////////
            //fill the first pipe
            ////////////////
            for(int count=0;count<batchSize;count++) {
            
                assertTrue(i+" "+count,PipeWriter.tryWriteFragment(pipeA, RawDataSchema.MSG_CHUNKEDSTREAM_1));        
                PipeWriter.writeUTF8(pipeA, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, Integer.toString(count));
                PipeWriter.publishWrites(pipeA);
                
            }
            
            ////////////////
            //copy everything over
            ////////////////
            int expectedBlobPos = -1;
            for(int count=0;count<batchSize;count++) {
                //grab teh target position before it gets moved, this is where we will validate the copied data.
                int bBlobPos = Pipe.getBlobWorkingHeadPosition(pipeB);
                
                if (expectedBlobPos>0) {//we keep a running total of where we think this values should be based on all the strings
                    assertEquals(expectedBlobPos, bBlobPos);
                }
                
                long bSlabPos = Pipe.workingHeadPosition(pipeB);
                
                assertTrue(PipeReader.tryReadFragment(pipeA));
                int len = PipeReader.readBytesLength(pipeA, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
                String expected = Integer.toString(count);
                assertEquals(expected.length(),len);
                
                
                assertTrue(PipeReader.tryMoveSingleMessage(pipeA, pipeB)); //this is where the copy happens and the code under test

                
                PipeReader.releaseReadLock(pipeA);            
                
                
                //Confirms that the bytes copied into the blob match what was expected.
                String afterCopy = Appendables.appendUTF8(new StringBuilder(), Pipe.blob(pipeB), bBlobPos, len, Pipe.blobMask(pipeB)).toString();
                assertEquals(expected,afterCopy);                    
  
                
                int copiedMsgIdx = Pipe.slab(pipeB)[  (int)(Pipe.slabMask(pipeB)&bSlabPos++) ];
                int copiedMeta   = Pipe.slab(pipeB)[  (int)(Pipe.slabMask(pipeB)&bSlabPos++) ];
                int copiedLen    = Pipe.slab(pipeB)[  (int)(Pipe.slabMask(pipeB)&bSlabPos++) ];
                
                assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, copiedMsgIdx);
                assertEquals(0, copiedMeta); //we only have one blob field and its always staring at zero.
                assertEquals(len, copiedLen);
                //compute where we expect blob to be next time to ensure the correct location.
                expectedBlobPos = (bBlobPos+copiedLen) & Pipe.BYTES_WRAP_MASK;
                
                //What is the base??
                
                
                                
            }
            
            //////////////
            //read and confirm the second pipe
            //////////////
            for(int count=0;count<batchSize;count++) {
                
                assertTrue(Integer.toString(count),PipeReader.tryReadFragment(pipeB));
                
                                
                assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, PipeReader.getMsgIdx(pipeB));
                
                String actual = PipeReader.readUTF8(pipeB, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, new StringBuilder()).toString();
                String expected = Integer.toString(count);
                assertEquals(i+" "+count,expected,actual);
                PipeReader.releaseReadLock(pipeB);
                
            }
            
        }
        
    }
    
    
}
