package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;

public class PipeSingleTemplateDecimalTest {

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
    	
		Pipe ring = new Pipe(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null, new MessageSchemaDynamic(FROM)));
    	ring.initBuffers();
    	
    	String emptyToString = ring.toString();
    	assertTrue(emptyToString, emptyToString.contains("slabHeadPos 0"));
    	assertTrue(emptyToString, emptyToString.contains("slabTailPos 0"));
    	assertTrue(emptyToString, emptyToString.contains("RingId"));
            	
        int messageSize = FROM.fragDataSize[FRAG_LOC];
        
        int varDataMax = (ring.byteMask/(ring.slabMask>>1))/messageSize;        
        int testSize = ((1<<primaryRingSizeInBits)/messageSize)-1; //reduce by one so we have room for the ending EOM value

        writeTestValue(ring, varDataMax, testSize);
        
        //now read the data back        
        int FIELD_LOC = FieldReferenceOffsetManager.lookupFieldLocator(SINGLE_MESSAGE_NAMES[0], FRAG_LOC, FROM);
        
        
        int k = testSize;
        while (PipeReader.tryReadFragment(ring)) {
        	--k;
        	assertTrue(PipeReader.isNewMessage(ring));
			int messageIdx = PipeReader.getMsgIdx(ring);
			if (messageIdx<0) {
				break;
			}
			readTestValue(ring, varDataMax, testSize, FIELD_LOC, k, messageIdx);
	        		 
        }    
    }

	private void readTestValue(Pipe pipe, int varDataMax, int testSize,
			int FIELD_LOC, int k, int messageIdx) {
		assertEquals(0, messageIdx);
		
		int expectedValue = ((varDataMax*(k))/testSize);		        	
		
		int exp = PipeReader.readDecimalExponent(pipe, FIELD_LOC);
		long man = PipeReader.readDecimalMantissa(pipe, FIELD_LOC);
		
		//System.err.println("read "+exp+" and "+man);
		
		float floatValue = PipeReader.readFloat(pipe, FIELD_LOC);
		assertEquals(floatValue+"",2, exp);
		assertEquals(floatValue+"",expectedValue,man);
	}

	private void writeTestValue(Pipe pipe, int blockSize, int testSize) {
		int j = testSize;
		
        while (true) {
        	
        	if (j == 0) {
        	
        		PipeWriter.publishEOF(pipe);
        		
        		return;//done
        	}
        
        	if (PipeWriter.tryWriteFragment(pipe, FRAG_LOC)) { //returns true if there is room to write this fragment
     		
        		int value = (--j*blockSize)/testSize;
        		
        		PipeWriter.writeDecimal(pipe, FRAG_FIELD, 2, (long) value );
        	
        		PipeWriter.publishWrites(pipe); //must always publish the writes if message or fragment
        		
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
    	final Pipe ring = new Pipe(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null, new MessageSchemaDynamic(FROM)));
    	ring.initBuffers();
    	
        final int messageSize = FROM.fragDataSize[FRAG_LOC];
        
        final int varDataMax = (ring.byteMask/(ring.slabMask>>1))/messageSize;        
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
	        if (PipeReader.tryReadFragment(ring)) { //this method releases old messages as needed and moves pointer up to the next fragment
	        	k--;//count down all the expected messages so we stop this test at the right time
	        	
	        	assertTrue(PipeReader.isNewMessage(ring));
				int messageIdx = PipeReader.getMsgIdx(ring);
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