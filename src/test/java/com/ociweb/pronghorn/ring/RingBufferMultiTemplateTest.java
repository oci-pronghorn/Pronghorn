package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;
import static com.ociweb.pronghorn.ring.RingWalker.isNewMessage;
import static com.ociweb.pronghorn.ring.RingWalker.messageIdx;
import static com.ociweb.pronghorn.ring.RingWalker.tryReadFragmentSimple;
import static com.ociweb.pronghorn.ring.RingWalker.tryWriteFragment;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;

public class RingBufferMultiTemplateTest {

	private static final byte[] ASCII_VERSION = "1.0".getBytes();

	private static final FieldReferenceOffsetManager FROM = buildFROM();
	
	private final int MSG_BOXES_LOC = lookupTemplateLocator("Boxes",FROM);  
	private final int MSG_SAMPLE_LOC = lookupTemplateLocator("Sample",FROM); 
	private final int MSG_RESET_LOC = lookupTemplateLocator("Reset",FROM);  
	
	private final int BOX_COUNT_LOC = lookupFieldLocator("Count", MSG_BOXES_LOC, FROM);
	private final int BOX_OWNER_LOC = lookupFieldLocator("Owner", MSG_BOXES_LOC, FROM);
    
	private final int SAMPLE_YEAR_LOC = lookupFieldLocator("Year", MSG_SAMPLE_LOC, FROM);
	private final int SAMPLE_MONTH_LOC = lookupFieldLocator("Month", MSG_SAMPLE_LOC, FROM);
	private final int SAMPLE_DATE_LOC = lookupFieldLocator("Date", MSG_SAMPLE_LOC, FROM);
	private final int SAMPLE_WEIGHT = lookupFieldLocator("Weight", MSG_SAMPLE_LOC, FROM);
    
	private final int REST_VERSION = lookupFieldLocator("Version", MSG_RESET_LOC, FROM);
    
    
	@Test
	public void startup() {
		
		assertEquals(3,FROM.messageStarts.length);
		
	}
	
	
	public static FieldReferenceOffsetManager buildFROM() {
		 
		String source = "/template/smallExample.xml";
		ClientConfig clientConfig = new ClientConfig();		
		TemplateCatalogConfig catalog = new TemplateCatalogConfig(TemplateLoader.buildCatBytes(source, clientConfig ));
		return catalog.getFROM();
		
	}
	
	
    @Test
    public void simpleBytesWriteRead() {
    
    	byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
    	byte byteRingSizeInBits = 16;
    	
		RingBuffer ring = new RingBuffer(primaryRingSizeInBits, byteRingSizeInBits, null,  FROM);
		
		//Setup the test data sizes derived from the templates used
		byte[] target = new byte[ring.maxAvgVarLen];
		
		
		int LARGEST_MESSAGE_SIZE = FROM.fragDataSize[MSG_SAMPLE_LOC];     //TODO: must mask this? 
        int testSize = (1<<primaryRingSizeInBits)/LARGEST_MESSAGE_SIZE;
        

        populateRingBuffer(ring, ring.maxAvgVarLen, testSize);

        
       //System.err.println("depth:"+Arrays.toString(FROM.fragDepth));
       
        //now read the data back
        int k = testSize;
        while (tryReadFragmentSimple(ring)) {
        	if (isNewMessage(ring)) {
        		
        		--k;
        		int expectedLength = (ring.maxAvgVarLen*k)/testSize;	
        		
        		int msgLoc = messageIdx(ring);
        		
        		//must cast for this test because the id can be 64 bits but we can only switch on 32 bit numbers
        		int templateId = (int)FROM.fieldIdScript[msgLoc];
        		       		
        	//	System.err.println("read TemplateID:"+templateId);
        		switch (templateId) {
	        		case 2:
	        		//	System.err.println("checking with "+k);
	        			
	        			assertEquals(MSG_BOXES_LOC,msgLoc);
	        			//reading out of order by design to ensure that random access works
	        			int ownLen = RingReader.readBytes(ring, BOX_OWNER_LOC, target, 0);
	        			assertEquals(expectedLength,ownLen);

	        			System.err.println("BOX LOC:"+Integer.toHexString(BOX_COUNT_LOC));
	        			int count = RingReader.readInt(ring, BOX_COUNT_LOC);
	        			assertEquals(42,count);
	        				        			
	        			break;
	        		case 1:
	        			assertEquals(MSG_SAMPLE_LOC,msgLoc);
	        			
	        			int year = RingReader.readInt(ring, SAMPLE_YEAR_LOC);
	        			assertEquals(2014,year);
	        			
	        			int month = RingReader.readInt(ring, SAMPLE_MONTH_LOC);
	        			assertEquals(12,month);
	        			
	        			int day = RingReader.readInt(ring, SAMPLE_DATE_LOC);
	        			assertEquals(9,day);
	        			
	        			long wMan = RingReader.readDecimalMantissa(ring, SAMPLE_WEIGHT);
	        			assertEquals(123456,wMan);
	        			
	        			int wExp = RingReader.readDecimalExponent(ring, SAMPLE_WEIGHT);
	        			assertEquals(2,wExp);	        			
	        			
	        			break;
	        		case 4:
	        			assertEquals(MSG_RESET_LOC,msgLoc);
	        			int verLen = RingReader.readBytes(ring, REST_VERSION, target, 0);
	        			assertEquals(3,verLen);	
	        			
	        			break;
	        		default:
	        			fail("Unexpected templateId of "+templateId);
	        			break;
        		
        		}
        		        		
        	} else {
        		fail("All fragments are messages for this test.");
        	}
        }    
    }

	private void populateRingBuffer(RingBuffer ring, int blockSize, int testSize) {
		
		int[] templateIds = new int[] {2,1,4};
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		return;//done
        	}
        	
        	//for this test we just round robin the message types.
        	int selectedTemplateId  =  templateIds[j%templateIds.length];
        	
        	//System.err.println("write template:"+selectedTemplateId);
        	
        	switch(selectedTemplateId) {
	        	case 2: //boxes
	        		if (tryWriteFragment(ring, MSG_BOXES_LOC)) { //AUTO writes template id as needed
		        		j--;
		        		
		        		//TODO: AAAA, these writes are using the working offset.
		        		
		        		RingBuffer.addValue(ring.buffer, ring.mask, ring.workingHeadPos, 42);
						byte[] source = buildMockData((j*blockSize)/testSize);
		        		RingBuffer.addByteArray(source, 0, source.length, ring);   
		        		
		        		//System.err.println(j+" wrote length:"+source.length);
		        				        		
		        		RingBuffer.publishWrites(ring); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	            		Thread.yield();
	            		
	            	}       
	        		break;
	        	case 1: //samples
	        		if (tryWriteFragment(ring, MSG_SAMPLE_LOC)) { 
		        		j--;
		        				        		
		        		RingBuffer.addValue(ring.buffer, ring.mask, ring.workingHeadPos, 2014);
		        		RingBuffer.addValue(ring.buffer, ring.mask, ring.workingHeadPos, 12);
		        		RingBuffer.addValue(ring.buffer, ring.mask, ring.workingHeadPos, 9);
		        		RingBuffer.addValues(ring.buffer, ring.mask, ring.workingHeadPos, 2, (long) 123456);
		        		
		        		RingBuffer.publishWrites(ring); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	        			Thread.yield();
	            		
	            	}  
	        		break;
	        	case 4: //reset
	        		if (tryWriteFragment(ring, MSG_RESET_LOC)) { 
	        			j--;
	        			
	        			RingBuffer.addByteArray(ASCII_VERSION, 0, ASCII_VERSION.length, ring);
		        		RingBuffer.publishWrites(ring); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	        			Thread.yield();
	            		
	            	}  
	        		break;
        	}        	
        	
        }
	}


	private byte[] buildMockData(int size) {
		byte[] result = new byte[size];
		int i = size;
		while (--i>=0) {
			result[i] = (byte)i;
		}
		return result;
	}
	
}
