package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFragmentLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.RingWriter;

public class StreamParserTest {

	private static final byte[] ASCII_VERSION = "1.0".getBytes();

	private static final FieldReferenceOffsetManager FROM = buildFROM();
	
	private final int MSG_BOXES_LOC = lookupTemplateLocator("Boxes",FROM);  
	private final int MSG_SAMPLE_LOC = lookupTemplateLocator("Sample",FROM); 
	private final int MSG_RESET_LOC = lookupTemplateLocator("Reset",FROM);  
	private final int MSG_TRUCKS_LOC = lookupTemplateLocator("TrucksMark2",FROM); 
	
	private final int BOX_COUNT_LOC = lookupFieldLocator("Count", MSG_BOXES_LOC, FROM);
	private final int BOX_OWNER_LOC = lookupFieldLocator("Owner", MSG_BOXES_LOC, FROM);
    
	private final int SAMPLE_YEAR_LOC = lookupFieldLocator("Year", MSG_SAMPLE_LOC, FROM);
	private final int SAMPLE_MONTH_LOC = lookupFieldLocator("Month", MSG_SAMPLE_LOC, FROM);
	private final int SAMPLE_DATE_LOC = lookupFieldLocator("Date", MSG_SAMPLE_LOC, FROM);
	private final int SAMPLE_WEIGHT = lookupFieldLocator("Weight", MSG_SAMPLE_LOC, FROM);
    
	private final int REST_VERSION = lookupFieldLocator("Version", MSG_RESET_LOC, FROM);
    
	private final int SQUAD_NAME = lookupFieldLocator("Squad", MSG_TRUCKS_LOC, FROM);	
	private final int SQUAD_NO_MEMBERS = lookupFieldLocator("NoMembers", MSG_TRUCKS_LOC, FROM);
	
	private final int MSG_TRUCK_SEQ_LOC = lookupFragmentLocator("Members", MSG_TRUCKS_LOC, FROM);
	private final int SQUAD_TRUCK_ID = lookupFieldLocator("TruckId", MSG_TRUCK_SEQ_LOC, FROM);
	private final int TRUCK_CAPACITY = lookupFieldLocator("Capacity", MSG_TRUCK_SEQ_LOC, FROM);
	
	private final int THING_NO_LOC = lookupFieldLocator("NoThings", MSG_TRUCK_SEQ_LOC, FROM);
	private final int MSG_TRUCK_THING_SEQ_LOC = lookupFragmentLocator("Things", MSG_TRUCK_SEQ_LOC, FROM);
	private final int THING_ID_LOC = lookupFieldLocator("AThing", MSG_TRUCK_THING_SEQ_LOC, FROM);
	
	public static FieldReferenceOffsetManager buildFROM() {
		 
		String source = "/template/smallExample.xml";
		TemplateCatalogConfig catalog = new TemplateCatalogConfig(TemplateLoader.buildCatBytes(source, new ClientConfig()));
		return catalog.getFROM();
		
	}
	
	@Test
	public void sequenceFragmentWriteRead() {
		byte primaryRingSizeInBits = 9; 
    	byte byteRingSizeInBits = 18;
    
    	
		RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
		
		int testSize = 5;
		
		//in this method we write two sequence members but only record the count after writing the members
		populateRingBufferWithSequence(ring, testSize);
		
//		0 Group/OpenTemplPMap/3
//		1 IntegerUnsigned/None/0
//		2 ASCII/Copy/0
//		3 Group/ClosePMap/3
		
//		4 Group/OpenTemplPMap/6
//		5 IntegerUnsigned/Copy/1
//		6 IntegerUnsigned/Copy/2
//		7 IntegerUnsigned/Copy/3
//		8 Decimal/Default/4
//		9 LongSigned/Delta/0
//		10 Group/ClosePMap/6
		
//		11 Group/OpenTempl/2
//		12 ASCII/Constant/1
//		13 Group/Close/2
//		14 Group/OpenTemplPMap/8
//		15 ASCII/Copy/2
//		16 Length/None/5
		
//		17 Group/OpenSeqPMap/4
//		18 LongUnsigned/None/1
//		19 Decimal/Default/6
//		20 LongSigned/Delta/2
//		21 Group/CloseSeqPMap/4
//		22 Group/ClosePMap/8
		
		StreamingConsumer visitor = new StreamingConsumerToJSON(System.out); 
		
		StreamingConsumerReader parser = new StreamingConsumerReader(ring, visitor );
		
		//ring is fully populated so we should not need to call this run again.s
		parser.run();

//		
//		
//		//Ring is full of messages, this loop runs until the ring is empty.
//        while (RingReader.tryReadFragment(ring)) {
//        	assertTrue(RingReader.isNewMessage(ring));
//
//        	int msgIdx = RingReader.getMsgIdx(ring);
//        	if (msgIdx<0) {
//        		break;
//        	}
//			assertEquals(MSG_TRUCKS_LOC, msgIdx);
//
//			assertEquals("TheBobSquad", RingReader.readASCII(ring, SQUAD_NAME, new StringBuilder()).toString());
//			
//			int sequenceCount = RingReader.readInt(ring, SQUAD_NO_MEMBERS);
//			assertEquals(2,sequenceCount);
//        	
//					
//			//now we now that we have 2 fragments to read
//			RingReader.tryReadFragment(ring);
//			assertEquals(10, RingReader.readLong(ring, SQUAD_TRUCK_ID));
//			assertEquals(2000, RingReader.readDecimalMantissa(ring, TRUCK_CAPACITY));
//			assertEquals(2, RingReader.readDecimalExponent(ring, TRUCK_CAPACITY));
//			assertEquals(20.00d, RingReader.readDouble(ring, TRUCK_CAPACITY),.001);
//        	
//			RingReader.tryReadFragment(ring);
//			assertEquals(11, RingReader.readLong(ring, SQUAD_TRUCK_ID));
//			assertEquals(3000, RingReader.readDecimalMantissa(ring, TRUCK_CAPACITY));
//			assertEquals(2, RingReader.readDecimalExponent(ring, TRUCK_CAPACITY));
//			assertEquals(30.00d, RingReader.readDouble(ring, TRUCK_CAPACITY),.001);
//        	
//        }
		
	}
	
	
	
	
	private void populateRingBufferWithSequence(RingBuffer ring, int testSize) {
		
		int j = testSize;
        while (true) {
        	
        	if (j==0) {
        		RingWriter.publishEOF(ring);
        		return;//done
        	}
        	        	
        	if (RingWriter.tryWriteFragment(ring, MSG_TRUCKS_LOC)) { //AUTO writes template id as needed
 
        		RingWriter.writeASCII(ring, SQUAD_NAME, "TheBobSquad");     		
        		
        		//WRITE THE FIRST MEMBER OF THE SEQ
        		//block to ensure we have room for the next fragment, and ensure that bytes consumed gets recorded
        		RingWriter.blockWriteFragment(ring, MSG_TRUCK_SEQ_LOC);//could use tryWrite here but it would make this example more complex
        		
        		RingWriter.writeLong(ring, SQUAD_TRUCK_ID, 10);         
        		RingWriter.writeDecimal(ring, TRUCK_CAPACITY, 2, 2000);
        		RingWriter.writeInt(ring, THING_NO_LOC, 1);
     
        		RingWriter.blockWriteFragment(ring, MSG_TRUCK_THING_SEQ_LOC);
        		RingWriter.writeInt(ring, THING_ID_LOC, 7);
        		//
        		
        		//WRITE THE SECOND MEMBER OF THE SEQ
        		//block to ensure we have room for the next fragment, and ensure that bytes consumed gets recorded
        		RingWriter.blockWriteFragment(ring, MSG_TRUCK_SEQ_LOC);
        		
        		RingWriter.writeLong(ring, SQUAD_TRUCK_ID, 11);
        		RingWriter.writeDouble(ring, TRUCK_CAPACITY, 30d, 2); //alternate way of writing a decimal
        		RingWriter.writeInt(ring, THING_NO_LOC, 1);
   
        		RingWriter.blockWriteFragment(ring, MSG_TRUCK_THING_SEQ_LOC);
        		RingWriter.writeInt(ring, THING_ID_LOC, 7);
        		
        		//NOTE: because we are waiting until the end of the  sequence to write its length we have two rules
        		//      1. Publish can not be called between these fragments because it will publish a zero for the count
        		//      2. The RingBuffer must be large enough to hold all the fragments in the sequence.
        		//      Neither one of these apply when the length can be set first.
        		
        		RingWriter.writeInt(ring, SQUAD_NO_MEMBERS, 2); //NOTE: we are writing this field very late because we now know how many we wrote.
        		
        		
        	//	RingWriter.blockWriteFragment(ring, MSG_TRUCK_AGE_FRAG_LOC);
        		
        //		RingWriter.writeLong(ring, SQUAD_AGE, 42);
        		
        		RingWriter.publishWrites(ring);
        		        		
        		 j--;       		
    		} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.yield();
        		
        	}     
        }
	}
	
}
