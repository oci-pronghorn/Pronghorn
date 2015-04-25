package com.ociweb.pronghorn.ring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class RingBufferSingleTemplateDecimalTest {

	//NOTE: when build a script by hand the Decimals requires 2 slots one for the decimal and the second a long
    private static int[] SINGLE_MESSAGE_TOKENS = new int[]{TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_None, 0),
    													   TokenBuilder.buildToken(TypeMask.LongSigned, OperatorMask.Field_None, 0),
    													   };
	private static String[] SINGLE_MESSAGE_NAMES = new String[]{"Decimal"};
	private static long[] SINGLE_MESSAGE_IDS = new long[]{0};
	private static final short ZERO_PREMABLE = 0;
	public static final FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(SINGLE_MESSAGE_TOKENS, 
	              ZERO_PREMABLE, 
	              SINGLE_MESSAGE_NAMES, 
	              SINGLE_MESSAGE_IDS);
	
	final int FRAG_LOC = 0;
	final int FRAG_FIELD = FieldReferenceOffsetManager.lookupFieldLocator(SINGLE_MESSAGE_NAMES[0], 0, FROM);
	
    @Test
    public void simpleWriteRead() {
        	
    	byte primaryRingSizeInBits = 9; 
    	byte byteRingSizeInBits = 17;
    	
		RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
    	ring.initBuffers();
        int messageSize = FROM.fragDataSize[FRAG_LOC];
        
        int varDataMax = (ring.byteMask/(ring.mask>>1))/messageSize;        
        int testSize = ((1<<primaryRingSizeInBits)/messageSize)-1; //reduce by one so we have room for the ending EOM value

        writeTestValue(ring, varDataMax, testSize);
        
        //now read the data back        
        int FIELD_LOC = FieldReferenceOffsetManager.lookupFieldLocator(SINGLE_MESSAGE_NAMES[0], FRAG_LOC, FROM);
        
        
        int k = testSize;
        while (RingReader.tryReadFragment(ring)) {
        	--k;
        	assertTrue(RingReader.isNewMessage(ring));
			int messageIdx = RingReader.getMsgIdx(ring);
			if (messageIdx<0) {
				break;
			}
			readTestValue(ring, varDataMax, testSize, FIELD_LOC, k, messageIdx);
	        		 
        }    
    }

	private void readTestValue(RingBuffer ring, int varDataMax, int testSize,
			int FIELD_LOC, int k, int messageIdx) {
		assertEquals(0, messageIdx);
		
		int expectedValue = ((varDataMax*(k))/testSize);		        	
		
		int exp = RingReader.readDecimalExponent(ring, FIELD_LOC);
		long man = RingReader.readDecimalMantissa(ring, FIELD_LOC);
		
		//System.err.println("read "+exp+" and "+man);
		
		float floatValue = RingReader.readFloat(ring, FIELD_LOC);
		assertEquals(floatValue+"",2, exp);
		assertEquals(floatValue+"",expectedValue,man);
	}

	private void writeTestValue(RingBuffer ring, int blockSize, int testSize) {
		int j = testSize;
		
        while (true) {
        	
        	if (j == 0) {
        	
        		RingWriter.publishEOF(ring);
        		
        		return;//done
        	}
        
        	if (RingWriter.tryWriteFragment(ring, FRAG_LOC)) { //returns true if there is room to write this fragment
     		
        		int value = (--j*blockSize)/testSize;
        		
        		RingWriter.writeDecimal(ring, FRAG_FIELD, 2, (long) value );
        	
        		RingWriter.publishWrites(ring); //must always publish the writes if message or fragment
        		
        	} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.yield();
        	}        	
        	
        }
	}
    
    @Test
    public void simpleWriteReadThreaded() {
    
    	final byte primaryRingSizeInBits = 8; //this ring is 2^7 eg 128
    	final byte byteRingSizeInBits = 16;
    	final RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
    	ring.initBuffers();
    	
        final int messageSize = FROM.fragDataSize[FRAG_LOC];
        
        final int varDataMax = (ring.byteMask/(ring.mask>>1))/messageSize;        
        final int testSize = (1<<primaryRingSizeInBits)/messageSize;
                
    	Thread t = new Thread(new Runnable(){

			@Override
			public void run() {
				writeTestValue(ring, varDataMax, testSize);
			}}
			);
    	t.start();
        
        //now read the data back
         
        
        int FIELD_LOC = FieldReferenceOffsetManager.lookupFieldLocator(SINGLE_MESSAGE_NAMES[0], FRAG_LOC, FROM);

        int k = testSize;
        while (k>0) {
        	
        	//This is the example code that one would normally use.
        	
        	//System.err.println("content "+ring.contentRemaining(ring));
	        if (RingReader.tryReadFragment(ring)) { //this method releases old messages as needed and moves pointer up to the next fragment
	        	k--;//count down all the expected messages so we stop this test at the right time
	        	
	        	assertTrue(RingReader.isNewMessage(ring));
				int messageIdx = RingReader.getMsgIdx(ring);
				if (messageIdx<0) {
					break;
				}
				readTestValue(ring, varDataMax, testSize, FIELD_LOC, k, messageIdx);
	        	
	        } else {
	        	//unable to read so at this point
	        	//we can do other work and try again soon
	        	Thread.yield();
	        	
	        }
        }
                
        }    
}