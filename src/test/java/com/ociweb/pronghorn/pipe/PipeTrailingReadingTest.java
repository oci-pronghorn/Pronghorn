package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;

public class PipeTrailingReadingTest {

    @Test
    public void testRunLowLevel() {
     
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
    public void testMultiMessageConsumeLowLevel() {
    	
    	
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
	        assertEquals(baseHeadPos, Pipe.getWorkingBlobHeadPosition(dataPipe));
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
    
	
		
	@Test
	public void testByteSpanConsumeHighLevel() {
		
		int testCases = 2;
		while (--testCases >= 0) {
		
			Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(100, 40);
			pipe.initBuffers();
			
			int testSize = 400;
			byte[] testData = new byte[testSize];
			/////
			//build up our test data
			/////
			new Random().nextBytes(testData);
			
			//new clean empty pipe
			assertTrue(pipe.toString(),pipe.toString().contains(" 0/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 0"));
			
			/////
			//write the data to the pipe as a series of chunks.
			/////
			int u = 10;
			int step = testSize/u;
			int pos = 0;
			while (--u>=0) {
				RawDataSchema.publishChunkedStream(pipe, 
						                           testData, pos, 
												   step);
				pos+=step;
			}
			
			//filled pipe with 10 items each taking 4 positions in the slab
			assertTrue(pipe.toString(),pipe.toString().contains(" 40/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 400"));
			
			
			//////
			//read the data back and span the chunks
			//////
			DataInputBlobReader<RawDataSchema> fieldByteArray = null;
			while (PipeReader.tryReadFragment(pipe)) {
			    int msgIdx = PipeReader.getMsgIdx(pipe);
			    switch(msgIdx) {
			        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
			        	if (null==fieldByteArray) {
			        		//first call get stream
			        		fieldByteArray = PipeReader.inputStream(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
			        	} else {		        	
			        		//follow on calls grow the stream
			        		fieldByteArray.accumHighLevelAPIField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
			        	}
			        	
			        break;
			        case -1:
			            fail();
			        break;
			    }
			    PipeReader.readNextWithoutReleasingReadLock(pipe);
			}
			
			//pipe has no more data to read however the consumer is holding the tail position back
			assertTrue(pipe.toString(),pipe.toString().contains(" 40/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 400"));
			
			int dataAvail = fieldByteArray.available();
			assertEquals(400, dataAvail);
			
			assertTrue(fieldByteArray.equalBytes(testData));		
			
			
			boolean testWithAll = testCases==0;		
			if (testWithAll) {
				PipeReader.releaseAllPendingReadLock(pipe);
			} else {	
				PipeReader.releaseAllPendingReadLock(pipe, dataAvail);			
			}
						
			//pipe is fully empty and all the positions are moved forward
			assertTrue(pipe.toString(),pipe.toString().contains(" 0/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 400"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 400"));
			
						
			RawDataSchema.publishChunkedStream(pipe, testData, 0, step);
			
			//write one more item to the pipe
			assertTrue(pipe.toString(),pipe.toString().contains(" 4/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 44"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 400"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 440"));
			
			boolean ok = PipeReader.tryReadFragment(pipe);
			assertTrue(ok);
			
			DataInputBlobReader<RawDataSchema> fieldByteArray1 = PipeReader.inputStream(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
			
			PipeReader.releaseReadLock(pipe);
			
			//consumed all the data the pipe is now empty
			assertTrue(pipe.toString(),pipe.toString().contains(" 0/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 44"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 44"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 440"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 440"));
			
		}		
	}
	
	
	@Test
	public void testByteSpanConsumeHighLevelInterleaveReadWrite() {
		
		int testCases = 2;
		while (--testCases >= 0) {
		
			Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(100, 40);
			pipe.initBuffers();
			
			int testSize = 400;
			byte[] testData = new byte[testSize];
			/////
			//build up our test data
			/////
			new Random().nextBytes(testData);
			
			//new clean empty pipe
			assertTrue(pipe.toString(),pipe.toString().contains(" 0/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 0"));
			
			/////
			//write the data to the pipe as a series of chunks.
			/////
			int u = 10;
			final int step = testSize/u;
			int pos = 0;
			while (--u>=0) {
				RawDataSchema.publishChunkedStream(pipe, 
						                           testData, pos, 
												   step);
				pos+=step;
			}
			
			//filled pipe with 10 items each taking 4 positions in the slab
			assertTrue(pipe.toString(),pipe.toString().contains(" 40/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 0"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 400"));
			
			
			//////
			//read the data back and span the chunks
			//////
			DataInputBlobReader<RawDataSchema> fieldByteArray = null;
			int iteration = 0;
			int testPosition = 0;
			final int readLen = step*2;
			while (PipeReader.tryReadFragment(pipe)) {
			    int msgIdx = PipeReader.getMsgIdx(pipe);
			    int consumed = 0;
			    switch(msgIdx) {
			        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
			        	if (null==fieldByteArray) {
			        		//first call get stream
			        		fieldByteArray = PipeReader.inputStream(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
			        	} else {		        	
			        		//follow on calls grow the stream
			        		fieldByteArray.accumHighLevelAPIField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
			        	}
			        	
			        	//every other message we read two together
			        	if (1==(1&iteration++)) {
			        		
			        		//consume 2 messages at a time
			        		fieldByteArray.equalBytes(testData, testPosition, readLen);
			        		testPosition += readLen;
			        	    consumed = readLen;
			        	    
			        	    assertEquals(0, fieldByteArray.available());
			        	    
			        	    
			        	} else {
			        		consumed = 0;
			        	}
			        break;
			        case -1:
			            fail();
			        break;
			    }
			    //if the steam still has bytes do not release this for write over otherwise do release
			    PipeReader.readNextWithoutReleasingReadLock(pipe);
			    PipeReader.releaseAllPendingReadLock(pipe, consumed);

			}
			
			//pipe has no more data to read however the consumer is holding the tail position back
			assertTrue(pipe.toString(),pipe.toString().contains(" 0/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 400"));
			
			assertEquals(0, fieldByteArray.available());
						
			RawDataSchema.publishChunkedStream(pipe, testData, 0, step);
			
			//write one more item to the pipe
			assertTrue(pipe.toString(),pipe.toString().contains(" 4/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 40"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 44"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 400"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 440"));
			
			boolean ok = PipeReader.tryReadFragment(pipe);
			assertTrue(ok);
			
			DataInputBlobReader<RawDataSchema> fieldByteArray1 = PipeReader.inputStream(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
			
			PipeReader.releaseReadLock(pipe);
			
			//consumed all the data the pipe is now empty
			assertTrue(pipe.toString(),pipe.toString().contains(" 0/512"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabTailPos 44"));
			assertTrue(pipe.toString(),pipe.toString().contains("slabHeadPos 44"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobTailPos 440"));
			assertTrue(pipe.toString(),pipe.toString().contains("blobHeadPos 440"));
			
		}		
	}
	
    
}
