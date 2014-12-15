package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingWalker.tryReadFragment;
import static com.ociweb.pronghorn.ring.RingWalker.isNewMessage; 
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class RingBufferSingleTemplateFloatTest {

    private static int[] SINGLE_MESSAGE_TOKENS = new int[]{TokenBuilder.buildToken(TypeMask.IntegerUnsigned, //TODO: may want to make special type
																				            OperatorMask.Field_None, 
																				            0)};
	private static String[] SINGLE_MESSAGE_NAMES = new String[]{"Float"};
	private static long[] SINGLE_MESSAGE_IDS = new long[]{0};
	private static final short ZERO_PREMABLE = 0;
	public static final FieldReferenceOffsetManager FLOAT_SCRIPT = new FieldReferenceOffsetManager(SINGLE_MESSAGE_TOKENS, 
	              ZERO_PREMABLE, 
	              SINGLE_MESSAGE_NAMES, 
	              SINGLE_MESSAGE_IDS);
	
	
	final FieldReferenceOffsetManager FROM = FLOAT_SCRIPT;
	final int FRAG_LOC = 0;
	
    @Test
    public void simpleWriteRead() {
    
    	byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
    	byte byteRingSizeInBits = 16;
    	
		RingBuffer ring = new RingBuffer(primaryRingSizeInBits, byteRingSizeInBits, null,  FROM);
    	
        int messageSize = FROM.fragDataSize[FRAG_LOC];
        
        int varDataMax = (ring.byteMask/(ring.mask>>1))/messageSize;        
        int testSize = (1<<primaryRingSizeInBits)/messageSize;

        writeTestValue(ring, varDataMax, testSize);
        
        //now read the data back        
        int FIELD_LOC = FieldReferenceOffsetManager.lookupFieldLocator(SINGLE_MESSAGE_NAMES[0], FRAG_LOC, FROM);
        
        
        int k = testSize;
        while (tryReadFragment(ring)) {
        	--k;
        	testReadValue(ring, varDataMax, testSize, FIELD_LOC, k);
	        		 
        }    
    }

	private void testReadValue(RingBuffer ring, int varDataMax, int testSize,
			int FIELD_LOC, int k) {
		assertTrue(isNewMessage(ring));
		assertEquals(0, RingWalker.messageIdx(ring));
		
		float expectedValue = 1f/(float)((varDataMax*(k))/testSize);		        	
		float value = RingReader.readIntBitsToFloat(ring, FIELD_LOC);	        	
		assertEquals(expectedValue, value, .00001);
	}

	private void writeTestValue(RingBuffer ring, int blockSize, int testSize) {
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		return;//done
        	}
        
        	if (RingWalker.tryWriteFragment(ring, FRAG_LOC)) { //returns true if there is room to write this fragment
     		
        		int value = (--j*blockSize)/testSize;
        		        		
        		//because there is only 1 template we do not write the template id it is assumed to be zero.
        		//now we write the data for the message
        		RingWriter.writeFloatToIntBits(ring, 1f/(float)value);

        		RingBuffer.publishWrites(ring); //must always publish the writes if message or fragment
        		
        	} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.yield();
        	}        	
        	
        }
	}
    
    @Test
    public void simpleWriteReadThreaded() {
    
    	final byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
    	final byte byteRingSizeInBits = 16;
    	final RingBuffer ring = new RingBuffer(primaryRingSizeInBits, byteRingSizeInBits, null,  FROM);
    	
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
	        if (tryReadFragment(ring)) { //this method releases old messages as needed and moves pointer up to the next fragment
	        	k--;//count down all the expected messages so we stop this test at the right time

	        	testReadValue(ring, varDataMax, testSize, FIELD_LOC, k);
	        	
	        } else {
	        	//unable to read so at this point
	        	//we can do other work and try again soon
	        	Thread.yield();
	        	
	        }
        }
                
        }    
}