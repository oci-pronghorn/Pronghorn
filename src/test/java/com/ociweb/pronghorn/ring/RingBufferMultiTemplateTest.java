package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingWalker.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;

public class RingBufferMultiTemplateTest {

	static final FieldReferenceOffsetManager FROM = buildFROM();
	
	//TODO: need to add method on from to lookup fragment locs from the template names.
	static final int FRAG_BOXES_LOC = FROM.messageStarts[0];//NOT RIGHT BUT WORKS FOR NOW.
	static final int FRAG_SAMPLE_LOC = FROM.messageStarts[1];
	static final int FRAG_RESET_LOC = FROM.messageStarts[2];
	final int MAX_VAR_FIELDS_PER_MESSAGE = 1; //TODO: must compute from FROM
	final int SMALLEST_MESSAGE_SIZE = FROM.fragDataSize[FRAG_RESET_LOC];//TODO: must compute from FROM
	final int LARGEST_MESSAGE_SIZE = FROM.fragDataSize[FRAG_SAMPLE_LOC];//TODO: must compute from FROM
	
	
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
		
		int varDataMax = (MAX_VAR_FIELDS_PER_MESSAGE*(ring.byteMask/(ring.mask>>1)))/SMALLEST_MESSAGE_SIZE; //TODO: move this math into ring or from        
        byte[] target = new byte[varDataMax];
		
        
        int testSize = (1<<primaryRingSizeInBits)/LARGEST_MESSAGE_SIZE;

        populateRingBufferWithBytes(ring, varDataMax, testSize);

        //set up these as would normally be done in the constructor
        int BOX_COUNT_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Count", FRAG_BOXES_LOC, FROM);
        int BOX_OWNER_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Owner", FRAG_BOXES_LOC, FROM);
        
        int SAMPLE_YEAR_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Year", FRAG_SAMPLE_LOC, FROM);
        int SAMPLE_MONTH_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Month", FRAG_SAMPLE_LOC, FROM);
        int SAMPLE_DATE_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Date", FRAG_SAMPLE_LOC, FROM);
        int SAMPLE_WEIGHT = FieldReferenceOffsetManager.lookupFieldLocator("Weight", FRAG_SAMPLE_LOC, FROM);
        
        int REST_VERSION = FieldReferenceOffsetManager.lookupFieldLocator("Version", FRAG_RESET_LOC, FROM);

        
        //now read the data back
        int k = testSize;
        while (tryReadFragment(ring)) {
        	if (isNewMessage(ring)) {
        		--k;
        		int expectedLength = (varDataMax*k)/testSize;	
        		
        		int msgLoc = ring.consumerData.getMsgIdx();
        		//TODO: oops can only switch on int need to think about this.
        		int templateId = (int)ring.consumerData.from.fieldIdScript[msgLoc];
        		       		
        		switch (templateId) {
	        		case 2:
	        			
	        			int count = RingReader.readInt(ring, BOX_COUNT_LOC);
	        			assertEquals(42,count);
	        			
	        			int ownLen = RingReader.readBytes(ring, BOX_OWNER_LOC, target, 0);
	        			assertEquals(expectedLength,ownLen);	      
	        			
	        			break;
	        		case 1:
	        			
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
	        			
	        			int verLen = RingReader.readBytes(ring, REST_VERSION, target, 0);
	        			assertEquals(3,verLen);	
	        			
	        			break;
	        		default:
	        			fail("Unexpected templateId of "+templateId);
	        			break;
        		
        		}
        		        		
        	}
        }    
    }

	private void populateRingBufferWithBytes(RingBuffer ring, int blockSize, int testSize) {
		
		int[] templateIds = new int[] {2,1,4};
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		return;//done
        	}
        	
        	//for this test we just round robin the message types.
        	int selectedTemplateId  =  templateIds[j%templateIds.length];
        	
        	switch(selectedTemplateId) {
	        	case 2: //boxes
	        		if (tryWriteFragment(ring, FRAG_BOXES_LOC)) { //AUTO writes template id as needed
		        		j--;

		        		//TODO: unlike the reader the writer only supports sequential write of the fields (this is to be fixed very soon)
		        		
		        		RingWriter.writeInt(ring, 42);
		        		RingWriter.writeBytes(ring, buildMockData((j*blockSize)/testSize));       
		        		
		        		RingBuffer.publishWrites(ring); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	            		Thread.currentThread();
	            		
	            	}       
	        		break;
	        	case 1: //samples
	        		if (tryWriteFragment(ring, FRAG_SAMPLE_LOC)) { 
		        		j--;
		        			        			
		        		RingWriter.writeInt(ring, 2014);
		        		RingWriter.writeInt(ring, 12);
		        		RingWriter.writeInt(ring, 9);
		        		RingWriter.writeDecimal(ring, 2, 123456);
		        		
		        		RingBuffer.publishWrites(ring); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	            		Thread.currentThread();
	            		
	            	}  
	        		break;
	        	case 4: //reset
	        		if (tryWriteFragment(ring, FRAG_RESET_LOC)) { 
	        			j--;
	        			RingWriter.writeBytes(ring, "1.0".getBytes()); //TODO: As written this is a bad idea, Needs to point to the constant build into the ring.     
		        		RingBuffer.publishWrites(ring); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	            		Thread.currentThread();
	            		
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
