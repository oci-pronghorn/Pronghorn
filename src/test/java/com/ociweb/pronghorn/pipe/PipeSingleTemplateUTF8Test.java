package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;

public class PipeSingleTemplateUTF8Test {

	final FieldReferenceOffsetManager FROM = RawDataSchema.FROM;
	final int FRAG_LOC = RawDataSchema.MSG_CHUNKEDSTREAM_1;
	
	final byte primaryRingSizeInBits = 3; 
	final byte byteRingSizeInBits = 22;
	
    @Test
    public void simpleBytesWriteRead() {
        	
		Pipe<RawDataSchema> ring = new Pipe<RawDataSchema>(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null,  RawDataSchema.instance));
		ring.initBuffers();
    	        
        int varDataMax = ring.maxAvgVarLen / 5; //fewer chars for UTF8        
        int testSize = (1<<byteRingSizeInBits)/ring.maxAvgVarLen; 
        
        populateRingBufferWithUTF8(ring, varDataMax, testSize);
        
                
        StringBuilder target = new StringBuilder();
        char[] target2 = new char[varDataMax << 1]; //HACK
        
        int k = testSize;
        while (PipeReader.tryReadFragment(ring)) {
        	if (PipeReader.isNewMessage(ring)) {
        		target.setLength(0);
        		assertEquals(0, PipeReader.getMsgIdx(ring));
        		
	        	int expectedCharLength = (varDataMax*(--k))/testSize;
	        		        	
	        	String testString = buildTestString(expectedCharLength);
	        	assert(testString.length()==expectedCharLength);
	        	
	        	
	        	
	        	if (0==(k&1)) {
		        	int actualLength = ((StringBuilder)PipeReader.readUTF8(ring, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, target)).length();
		        	assertEquals(expectedCharLength,actualLength);
		        	assertEquals(testString,target.toString());
	        	} else {
		        	int actualLength = PipeReader.readUTF8(ring, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, target2, 0);
		        	assertEquals(expectedCharLength,actualLength);
		        	
		        	int j = expectedCharLength;
		        	while (--j>=0) {
		        		int expectedChar = (int)testString.charAt(j);
		        		int computedChar = (int)target2[j];
		        		if (expectedChar!=computedChar) {
		        			
		        			//special case, these are utf-16 reserved chars and the utf-8 encoder will turn them into 63
		        			if (computedChar==63 && (0xD800==(0xF800&expectedChar))) {
		        				//not an error
		        			} else {
		        				fail("exp:"+testString+" vs \nfnd:"+new String(Arrays.copyOfRange(target2, 0, expectedCharLength))   );	        		
		        			}
		        		}		        		
		        	}
	        	}        	
	        	
        	}
        }    
    }

	private void populateRingBufferWithUTF8(Pipe pipe, int blockSize, int testSize) {
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		return;//done
        	}
        
        	if (PipeWriter.tryWriteFragment(pipe, FRAG_LOC)) { //returns true if there is room to write this fragment
        	    Pipe.writeTrailingCountOfBytesConsumed(pipe, FRAG_LOC);
        		int stringSize = (--j*blockSize)/testSize;
        		
        		String testString = buildTestString(stringSize);
        		char[] testChars = testString.toCharArray();
        		
        		//because there is only 1 template we do not write the template id it is assumed to be zero.
        		//now we write the data for the message
        		if (0 == (j&1)) {
        			PipeWriter.writeUTF8(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, testString);
      
        		} else {
        			if (0 == (j&2)) {
        				PipeWriter.writeUTF8(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, testChars);
      
        			} else {
        				PipeWriter.writeUTF8(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, testChars, 0, stringSize);
    
        			}
        		}
        		Pipe.publishWritesBatched(pipe); //must always publish the writes if message or fragment
        		
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
			arrayData[i] = (char)((11*i)&0xFFFFFFF);
		}
		return new String(arrayData);
	}
    
    @Test
    public void simpleBytesWriteReadThreaded() {
    

    	final Pipe<RawDataSchema> ring = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(primaryRingSizeInBits, byteRingSizeInBits, null,  RawDataSchema.instance));
    	ring.initBuffers();
    	
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
	        if (PipeReader.tryReadFragment(ring)) { //this method releases old messages as needed and moves pointer up to the next fragment
	        	x=0;
	        	k--;//count down all the expected messages so we stop this test at the right time
	        	target.setLength(0);
	        	assertTrue(PipeReader.isNewMessage(ring));//would use this method rarely to determine if fragment starts new message
	        	assertEquals(0, PipeReader.getMsgIdx(ring)); //when we only have 1 message type this would not normally be called

	        	int expectedLength = (varDataMax*k)/testSize;		        	
	        	String testString = buildTestString(expectedLength);
	        	
	        	if (0==(k&2)) {
		        	int actualLength = ((StringBuilder)PipeReader.readUTF8(ring, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, target)).length();
		        	assertEquals(expectedLength,actualLength);	
		        	int j = expectedLength;
		        	while (--j>=0) {
		        		int expectedChar = (int)testString.charAt(j);
		        		int computedChar = (int)target.charAt(j);
		        		if (expectedChar!=computedChar) {
		        			
		        			//special case, these are utf-16 reserved chars and the utf-8 encoder will turn them into 63
		        			if (computedChar==63 && (0xD800==(0xF800&expectedChar))) {
		        				//not an error
		        			} else {
		        				fail("exp:"+testString+" vs \nfnd:"+new String(Arrays.copyOfRange(target2, 0, expectedLength))   );	        		
		        			}
		        		}		        		
		        	}
	        	}  else {
	        		int actualLength = PipeReader.readUTF8(ring, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, target2, 0);
		        	assertEquals(expectedLength,actualLength);
		        	
		        	int j = expectedLength;
		        	while (--j>=0) {
		        		int expectedChar = (int)testString.charAt(j);
		        		int computedChar = (int)target2[j];
		        		if (expectedChar!=computedChar) {
		        			
		        			//special case, these are utf-16 reserved chars and the utf-8 encoder will turn them into 63
		        			if (computedChar==63 && (0xD800==(0xF800&expectedChar))) {
		        				//not an error
		        			} else {
		        				fail("exp:"+testString+" vs \nfnd:"+new String(Arrays.copyOfRange(target2, 0, expectedLength))   );	        		
		        			}
		        		}		        		
		        	}
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