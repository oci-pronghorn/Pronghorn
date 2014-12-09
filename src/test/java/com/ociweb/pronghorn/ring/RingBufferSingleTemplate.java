package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingWalker.tryReadFragment;
import static com.ociweb.pronghorn.ring.RingWalker.isNewMessage; 
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RingBufferSingleTemplate {

	final FieldReferenceOffsetManager rawBytesFROM = FieldReferenceOffsetManager.RAW_BYTES;
	final int FRAG_LOC = 0;
	
    @Test
    public void simpleBytesWriteRead() {
    
    	byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
    	byte byteRingSizeInBits = 16;
    	
		RingBuffer ring = new RingBuffer(primaryRingSizeInBits, byteRingSizeInBits, null,  rawBytesFROM);
    	
        int messageSize = rawBytesFROM.fragDataSize[FRAG_LOC];
        
        int blockSize = (ring.byteMask/(ring.mask>>1))/messageSize;        
        int testSize = (1<<primaryRingSizeInBits)/messageSize;

        populateRingBuffer(ring, blockSize, testSize);
        
        //now read the data back        
        int BYTE_LOC = FieldReferenceOffsetManager.lookupFieldLocator("ByteArray", FRAG_LOC, rawBytesFROM);
        
        byte[] target = new byte[blockSize];
        int k = testSize;
        while (tryReadFragment(ring)) {
        	if (isNewMessage(ring)) {
        		assertEquals(0, ring.consumerData.getMsgIdx());
        		
	        	int expectedLength = (blockSize*(--k))/testSize;	
	        	int actualLength = RingReader.readBytes(ring, BYTE_LOC, target, 0); //read bytes as normal code would do
	        	assertEquals(expectedLength,actualLength);
        		        		
        	}
        }    
    }

	private void populateRingBuffer(RingBuffer ring, int blockSize, int testSize) {
		int j = testSize;
        while (true) {
        	
        	if (--j == 0) {
        		return;//done
        	}
        
        	int FRAG_LOC = 0;
        	if (RingWalker.tryWriteFragment(ring, FRAG_LOC)) { //returns true if there is room to write this fragment
        		        		
        		int arraySize = (blockSize*j)/testSize;
        		byte[] arrayData = buildTestData(arraySize);
        		        		
        		//because there is only 1 template we do not write the template id it is assumed to be zero.
        		//now we write the data for the message
        		RingWriter.writeBytes(ring, arrayData); 
        		RingBuffer.publishWrites(ring); //must always publish the writes if message or fragment
        		
        	} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.currentThread();
        		
        	}        	
        	
        }
	}

	private byte[] buildTestData(int arraySize) {
		byte[] arrayData = new byte[arraySize];
		int i = arrayData.length;
		while (--i >= 0) {
			arrayData[i] = (byte)i;
		}
		return arrayData;
	}
    
    @Test
    public void simpleBytesWriteReadThreaded() {
    
    	final byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
    	final byte byteRingSizeInBits = 16;
    	final RingBuffer ring = new RingBuffer(primaryRingSizeInBits, byteRingSizeInBits, null,  rawBytesFROM);
    	
        final int messageSize = rawBytesFROM.fragDataSize[FRAG_LOC];
        
        final int blockSize = (ring.byteMask/(ring.mask>>1))/messageSize;        
        final int testSize = (1<<primaryRingSizeInBits)/messageSize;
                
    	Thread t = new Thread(new Runnable(){

			@Override
			public void run() {
				populateRingBuffer(ring, blockSize, testSize);
			}}
			);
    	t.start();
        
        //now read the data back
         
        byte[] target = new byte[blockSize];
        
        int BYTE_LOC = 0; //TODO: need to load
        
//        int k = testSize;
//        while (k>0) {
//        	
//        	//This is the example code that one would normally use.
//        	
//        	System.err.println("content "+ring.contentRemaining(ring));
//	        if (tryReadFragment(ring)) { //this method releases old messages as needed and moves pointer up to the next fragment
//	        	k--;//count down all the expected messages so we stop this test at the right time
//
//	        	assertTrue(isNewMessage(ring));//would use this method rarely to determine if fragment starts new message
//	        	assertEquals(0, RingWalker.messageIdx(ring)); //when we only have 1 message type this would not normally be called
//
//	        	int expectedLength = (blockSize*k)/testSize;	
//	        	
//	        	System.err.println(k+" read len:"+expectedLength);
//	        	
//	        	int actualLength = RingReader.readBytes(ring, BYTE_LOC, target, 0); //read bytes as normal code would do
//	        	assertEquals(expectedLength,actualLength);
//	        	
//
//	        } else {
//	        	//unable to read so at this point
//	        	//we can do other work and try again soon
//	        	Thread.yield();
//	        	
//	        }
//        }
        
        //TODO: add test for a complex record type.
        
        }
    
}