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
        
        
        int totalBytesInFlight=0;
        
        int c = TOTAL_TEST_SIZE;
        while (--c>=0) {
            int dataLen = c&0x1FF;//  511
        
            ///////////////////////////////
            //Write one low level message
            ///////////////////////////////
      
            assertTrue(Pipe.hasRoomForWrite(dataPipe));
            
            int writeSize = Pipe.addMsgIdx(dataPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);            
            
                DataOutputBlobWriter.openField(blobWriter);
                //TODO: make up some data.
                blobWriter.write(testData, 0, dataLen);
                DataOutputBlobWriter.closeLowLevelField(blobWriter);

                totalBytesInFlight+=dataLen;
                
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
                
                blobReader.openLowLevelAPIField();
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
            
            ///////////////////////////////
            //consumer
            //////////////////////////////
            
            //do not release any until we reach this point.
            if (c<releaseTrigger) {
            	
                Pipe.releasePendingAsReadLock(dataPipe, bytesInMessage);
                totalBytesInFlight-=bytesInMessage;
            }
        
            int count = Pipe.releasePendingCount(dataPipe);
            
        }
        
        
    }
    
    @Test
    public void testMultiMessageConsume() {
    	
    	
        final int MSG_IN_FLIGHT = 1000;
        final int PIPE_PADDING_TO_FORCE_WRAP_ARROUND = 333;
        
        
        final int MSG_PAYLOAD_SIZE  = 100;
        
        PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, MSG_IN_FLIGHT+PIPE_PADDING_TO_FORCE_WRAP_ARROUND, MSG_PAYLOAD_SIZE);
        
        Pipe<RawDataSchema> dataPipe = new Pipe<RawDataSchema>(config);
        dataPipe.initBuffers();
        byte[] data = new byte[MSG_PAYLOAD_SIZE];
        
        int baseTailPos = 0;        
        int baseHeadPos = 0;
        
        int iterations = 30;
        while (--iterations>=0) {
        
        
	    	///////////////////////////
	    	//write data as small blocks
	    	///////////////////////////
	    	for(int i = 0;i<MSG_IN_FLIGHT;i++) {
	    		assertTrue(Pipe.hasRoomForWrite(dataPipe));
	    		
	    		Pipe.addMsgIdx(dataPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	    		Pipe.addByteArray(data, 0, data.length, dataPipe);
	    		baseHeadPos+=data.length;
	    		Pipe.confirmLowLevelWrite(dataPipe, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
	    		Pipe.publishWrites(dataPipe);
	    	}
	        assertEquals(baseHeadPos, Pipe.getBlobWorkingHeadPosition(dataPipe));
	        assertEquals(baseHeadPos, Pipe.getBlobHeadPosition(dataPipe)); //can only read up to this point
	        
	        
	        //////////////////////////////
	    	//consume data as large blocks
	    	//////////////////////////////
	        {
	         int consumeBlockSize = MSG_PAYLOAD_SIZE + (MSG_PAYLOAD_SIZE/2);        
	         int accumulateCount = 600;
	         int consumeCount = 400;        
	                
	         baseTailPos = testAccumulateConsume(baseTailPos, MSG_PAYLOAD_SIZE, dataPipe, consumeBlockSize, accumulateCount, consumeCount);
	        }       
			//////////////////////////////
			//consume data as small blocks
			//////////////////////////////
	        {
	         int consumeBlockSize = MSG_PAYLOAD_SIZE/4;        
	         int accumulateCount = 400;
	         int consumeCount = accumulateCount*4;        
	                
	         baseTailPos = testAccumulateConsume(baseTailPos, MSG_PAYLOAD_SIZE, dataPipe, consumeBlockSize, accumulateCount, consumeCount);
	        }
        }
    }

	private int testAccumulateConsume(final int baseTailPos, final int MSG_PAYLOAD_SIZE, Pipe<RawDataSchema> dataPipe, int consumeBlockSize,
			int accumulateCount, int consumeCount) {
		
		assertEquals(baseTailPos, Pipe.getWorkingTailPosition(dataPipe));
		
		final int expectedTailMovement = accumulateCount*Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		for(int i=0;i<accumulateCount;i++) {
        	
        	assertTrue(Pipe.hasContentToRead(dataPipe));
        	int msg = Pipe.takeMsgIdx(dataPipe);
        	int meta = Pipe.takeRingByteMetaData(dataPipe);
        	int len = Pipe.takeRingByteLen(dataPipe);
        	Pipe.bytePosition(meta, dataPipe, len);
        	
        	Pipe.confirmLowLevelRead(dataPipe, Pipe.sizeOf(RawDataSchema.instance, msg));
        	Pipe.readNextWithoutReleasingReadLock(dataPipe);
        	
        	assertEquals(i+1, Pipe.releasePendingCount(dataPipe));
        	
        }

        for(int i=0;i<consumeCount;i++) {        	
        	Pipe.releasePendingAsReadLock(dataPipe, consumeBlockSize);
        }
        //return new base
        return baseTailPos+expectedTailMovement;
	}
    
    
}
