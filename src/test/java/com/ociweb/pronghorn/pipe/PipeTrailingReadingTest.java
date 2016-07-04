package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

public class PipeTrailingReadingTest {

    @Test
    public void testRun() {
     
        final int TOTAL_TEST_SIZE = 100000;
        
        final int MSG_IN_FLIGHT = 100;
        final int MSG_MAX_SIZE  = 1000;
        
        PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, MSG_IN_FLIGHT, MSG_MAX_SIZE);
        
        Pipe<RawDataSchema> dataPipe = new Pipe<RawDataSchema>(config);
        dataPipe.initBuffers();
        
        byte[] testData = new byte[MSG_MAX_SIZE];
        byte[] targetData = new byte[MSG_MAX_SIZE];
        
             
        DataOutputBlobWriter<RawDataSchema> blobWriter = new DataOutputBlobWriter<RawDataSchema>(dataPipe);
        DataInputBlobReader<RawDataSchema> blobReader = new DataInputBlobReader<RawDataSchema>(dataPipe); 
        
        int releaseTrigger = TOTAL_TEST_SIZE - (MSG_IN_FLIGHT/2);
        long lastKnownTailPos = 0;
        
        int c = TOTAL_TEST_SIZE;
        while (--c>=0) {
            int dataLen = c&0x1FF;//  511
        
            ///////////////////////////////
            //Write one low level message
            ///////////////////////////////
      
            assertTrue(Pipe.hasRoomForWrite(dataPipe));
            
            int writeSize = Pipe.addMsgIdx(dataPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);            
            
            try {
                DataOutputBlobWriter.openField(blobWriter);
                //TODO: make up some data.
                blobWriter.write(testData, 0, dataLen);
                DataOutputBlobWriter.closeLowLevelField(blobWriter);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
            
            Pipe.confirmLowLevelWrite(dataPipe, writeSize);            
            Pipe.publishWrites(dataPipe);  
            
            ///////////////////////////////
            //Read one low level message
            ///////////////////////////////
        
            assertTrue(Pipe.hasContentToRead(dataPipe));
            
            ////////////////////////
            //confirm that tail position only goes forward by step  (known message size)
            ////////////////////////
            long workingTailPos = Pipe.getWorkingTailPosition(dataPipe);            
            long step = workingTailPos - lastKnownTailPos;
            if (step!=0 || workingTailPos!=0) {//only run test after first iteraiton
                assertEquals(Pipe.sizeOf(dataPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1),step);
            }
            lastKnownTailPos = workingTailPos;
            ////////////////////////
            
            int msgIdx = Pipe.takeMsgIdx(dataPipe);
                
            int bytesInMessage=0;
            try {                
                int peekLen = Pipe.peekInt(dataPipe, 1);
                
                DataInputBlobReader.openLowLevelAPIField(blobReader);
                int avail = blobReader.available();
                
                bytesInMessage = blobReader.read(targetData);
                if (bytesInMessage<0) {
                    bytesInMessage = 0;
                }
                String label = "test "+(TOTAL_TEST_SIZE-c);
                assertEquals(label, dataLen, peekLen);                
                assertEquals(label, dataLen, avail);                
                assertEquals(label, dataLen, bytesInMessage);
                
                blobReader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            
            Pipe.confirmLowLevelRead(dataPipe, Pipe.sizeOf(dataPipe, msgIdx));
            
            Pipe.readNextWithoutReleasingReadLock(dataPipe);
            
         //   Pipe.releaseReadLock(dataPipe);
            
            ///////////////////////////////
            //consumer
            //////////////////////////////
            
            //do not release any until we reach this point.
            if (c<releaseTrigger) {
                Pipe.releasePendingAsReadLock(dataPipe, bytesInMessage);
            }
        
           // int onPipe = (Pipe.contentRemaining(dataPipe)/Pipe.sizeOf(dataPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
         //   System.out.println(onPipe);
            
        }
        
        
    }
    
    
}
