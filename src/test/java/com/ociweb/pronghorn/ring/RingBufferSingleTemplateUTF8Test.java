package com.ociweb.pronghorn.ring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

public class RingBufferSingleTemplateUTF8Test {

	final FieldReferenceOffsetManager FROM = FieldReferenceOffsetManager.RAW_BYTES;
	final int FRAG_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
	
	final byte primaryRingSizeInBits = 8; 
	final byte byteRingSizeInBits = 19;
	
    @Ignore
    public void simpleBytesWriteRead() {//TODO: B, this unit test hangs, need to walk it slow and find the problem
        	
		RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
    	        
        int varDataMax = ring.maxAvgVarLen >> 3; //fewer chars for UTF8        
        int testSize = (1<<byteRingSizeInBits)/ring.maxAvgVarLen; 

        populateRingBufferWithUTF8(ring, varDataMax, testSize);
        
                
        StringBuilder target = new StringBuilder();
        char[] target2 = new char[varDataMax << 1]; //HACK
        
        int k = testSize;
        while (RingReader.tryReadFragment(ring)) {
        	if (RingReader.isNewMessage(ring)) {
        		target.setLength(0);;
        		assertEquals(0, RingReader.getMsgIdx(ring));
        		
	        	int expectedCharLength = (varDataMax*(--k))/testSize;
	        		        	
	        	String testString = buildTestString(expectedCharLength);
	        	assert(testString.length()==expectedCharLength);
	        	
	        	if (0==(k&1)) {
		        	int actualLength = ((StringBuilder)RingReader.readUTF8(ring, FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD, target)).length();
		        	assertEquals(expectedCharLength,actualLength);
		        	assertEquals(testString,target.toString());
	        	} else {
		        	int actualLength = RingReader.readUTF8(ring, FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD, target2, 0);
		        	assertEquals(expectedCharLength,actualLength);
		        	assertTrue("exp:"+testString+" vs \nfnd:"+new String(Arrays.copyOfRange(target2, 0, expectedCharLength)),		        			    
		        			    Arrays.equals(testString.toCharArray(), Arrays.copyOfRange(target2, 0, expectedCharLength) )
		        			   );	        		
	        	}        	
	        	
        	}
        }    
    }

	private void populateRingBufferWithUTF8(RingBuffer ring, int blockSize, int testSize) {
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		return;//done
        	}
        
        	if (RingWriter.tryWriteFragment(ring, FRAG_LOC)) { //returns true if there is room to write this fragment
     		
        		int stringSize = (--j*blockSize)/testSize;
        		
        		String testString = buildTestString(stringSize);
        		char[] testChars = testString.toCharArray();
        		
        		//because there is only 1 template we do not write the template id it is assumed to be zero.
        		//now we write the data for the message
        		if (0 == (j&1)) {
        			RingWriter.writeUTF8(ring, FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD, testString);
      
        		} else {
        			if (0 == (j&2)) {
        				RingWriter.writeUTF8(ring, FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD, testChars);
      
        			} else {
        				RingWriter.writeUTF8(ring, FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD, testChars, 0, stringSize);
    
        			}
        		}
        		RingBuffer.publishWrites(ring); //must always publish the writes if message or fragment
        		
        	} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.yield();
        	}        	
        	
        }
	}

	private String buildTestString(int arraySize) {
		char[] arrayData = new char[arraySize];
		int i = arrayData.length;
		while (--i >= 0) {
			arrayData[i] = (char)(i&0xFFFF);//short
		}
		return new String(arrayData);
	}
    
    @Test
    public void simpleBytesWriteReadThreaded() {
    

    	final RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
    	        
        final int varDataMax = ring.maxAvgVarLen >> 3; //fewer chars for UTF8        
        final int testSize = (1<<byteRingSizeInBits)/ring.maxAvgVarLen; 
                
    	Thread t = new Thread(new Runnable(){

			@Override
			public void run() {
				populateRingBufferWithUTF8(ring, varDataMax, testSize);
			}}
			);
    	t.start();
        
        //now read the data back
         
    	StringBuilder target = new StringBuilder();
    	char[] target2 = new char[varDataMax];
        
        int x = 0;
        int k = testSize;
        while (k>1) {
        	
        	//This is the example code that one would normally use.
        	
        	//System.err.println("content "+ring.contentRemaining(ring));
	        if (RingReader.tryReadFragment(ring)) { //this method releases old messages as needed and moves pointer up to the next fragment
	        	x=0;
	        	k--;//count down all the expected messages so we stop this test at the right time
	        	target.setLength(0);
	        	assertTrue(RingReader.isNewMessage(ring));//would use this method rarely to determine if fragment starts new message
	        	assertEquals(0, RingReader.getMsgIdx(ring)); //when we only have 1 message type this would not normally be called

	        	int expectedLength = (varDataMax*k)/testSize;		        	
	        	String testString = buildTestString(expectedLength);
	        	
	        	if (0==(k&2)) {
		        	int actualLength = ((StringBuilder)RingReader.readUTF8(ring, FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD, target)).length();
		        	assertEquals(expectedLength,actualLength);	
		        	assertEquals(testString,target.toString());
	        	}  else {
	        		int actualLength = RingReader.readUTF8(ring, FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD, target2, 0);
		        	assertEquals(expectedLength,actualLength);
		        	assertTrue(testString+" vs "+new String(target2, 0, actualLength),		        			    
		        			    Arrays.equals(testString.toCharArray(), 
		        			                 Arrays.copyOfRange(target2, 0, actualLength)
		        			                 )
		        			   );	
	        	}
	        } else {
	        	if (++x>1000000) {
	        		fail("Unable to finish stuck on "+k+" down from "+testSize);
	        	}
	        	//unable to read so at this point
	        	//we can do other work and try again soon
	        	Thread.yield();
	        	
	        }
        }
                
        }    
}