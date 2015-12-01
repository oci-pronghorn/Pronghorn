package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class RingBufferSingleTemplateTest {

	final FieldReferenceOffsetManager FROM = RawDataSchema.FROM;
	final int FRAG_LOC = RawDataSchema.MSG_CHUNKEDSTREAM_1;
	final int FRAG_FIELD = RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2;
	
    @Test
    public void simpleBytesWriteRead() {
    
    	byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
    	byte byteRingSizeInBits = 16;
    	
		Pipe ring = new Pipe(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null,  RawDataSchema.instance));
    	ring.initBuffers();
    	
        int messageSize = FROM.fragDataSize[FRAG_LOC];
        
        int varDataMax = (ring.byteMask/(ring.mask>>1))/messageSize;        
        int testSize = (1<<primaryRingSizeInBits)/messageSize;

        populateRingBufferWithBytes(ring, varDataMax, testSize);
        
        //now read the data back        
        int BYTE_LOC = FieldReferenceOffsetManager.lookupFieldLocator("ByteArray", FRAG_LOC, FROM);
        
        byte[] target = new byte[varDataMax];
        int k = testSize;
        while (PipeReader.tryReadFragment(ring)) {
        	if (PipeReader.isNewMessage(ring)) {
        		assertEquals(0, PipeReader.getMsgIdx(ring));
        		
	        	int expectedLength = (varDataMax*(--k))/testSize;	
	        	int actualLength = PipeReader.readBytes(ring, BYTE_LOC, target, 0); //read bytes as normal code would do
	        	assertEquals(expectedLength,actualLength);
        		        		
        	}
        }    
    }

	private void populateRingBufferWithBytes(Pipe ring, int blockSize, int testSize) {
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		return;//done
        	}

        	if (PipeWriter.tryWriteFragment(ring,FRAG_LOC)) { //returns true if there is room to write this fragment
        	    Pipe.writeTrailingCountOfBytesConsumed(ring, FRAG_LOC);
        		int arraySize = (--j*blockSize)/testSize;
        		byte[] arrayData = buildTestData(arraySize);
        		        		
        		PipeWriter.writeBytes(ring, FRAG_FIELD, arrayData);
        		
        		//because there is only 1 template we do not write the template id it is assumed to be zero.
        		//now we write the data for the message        		
        		Pipe.publishWritesBatched(ring); //must always publish the writes if message or fragment
        		
        	} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.yield();
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
    	final Pipe ring = new Pipe(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null,  RawDataSchema.instance));
    	ring.initBuffers();
    	
        final int messageSize = FROM.fragDataSize[FRAG_LOC];
        
        final int varDataMax = (ring.byteMask/(ring.mask>>1))/messageSize;        
        final int testSize = (1<<primaryRingSizeInBits)/messageSize;
                
    	Thread t = new Thread(new Runnable(){

			@Override
			public void run() {
				populateRingBufferWithBytes(ring, varDataMax, testSize);
			}}
			);
    	t.start();
        
        //now read the data back
         
        byte[] target = new byte[varDataMax];
        
        int BYTE_LOC = FieldReferenceOffsetManager.lookupFieldLocator("ByteArray", FRAG_LOC, FROM);
        
        int k = testSize;
        while (k>1) {
        	
        	//This is the example code that one would normally use.
        	
        	//System.err.println("content "+ring.contentRemaining(ring));
	        if (PipeReader.tryReadFragment(ring)) { //this method releases old messages as needed and moves pointer up to the next fragment
	        	k--;//count down all the expected messages so we stop this test at the right time

	        	assertTrue(PipeReader.isNewMessage(ring));//would use this method rarely to determine if fragment starts new message
	        	assertEquals(0, PipeReader.getMsgIdx(ring)); //when we only have 1 message type this would not normally be called

	        	int expectedLength = (varDataMax*k)/testSize;		        	
	        	int actualLength = PipeReader.readBytes(ring, BYTE_LOC, target, 0); //read bytes as normal code would do
	        	assertEquals(expectedLength,actualLength);	        	

	        } else {
	        	//unable to read so at this point
	        	//we can do other work and try again soon
	        	Thread.yield();
	        	
	        }
        }
                
        }    
}