package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFragmentLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.ring.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.ring.stream.StreamingWriteVisitorGenerator;

public class RingBufferMultiTemplateTest {

	private static final byte[] ASCII_VERSION = "1.0".getBytes();

	private static final FieldReferenceOffsetManager FROM = buildFROM();
	
	private final int MSG_BOXES_LOC = lookupTemplateLocator("Boxes",FROM);  
	private final int MSG_SAMPLE_LOC = lookupTemplateLocator("Sample",FROM); 
	private final int MSG_RESET_LOC = lookupTemplateLocator("Reset",FROM);  
	private final int MSG_TRUCKS_LOC = lookupTemplateLocator("Trucks",FROM); 
	
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
	
    
	@Test
	public void startup() {
		int messageTypeCount = 5;
		assertEquals(messageTypeCount,FROM.messageStarts.length);
		
	}
	
	
	public static FieldReferenceOffsetManager buildFROM() {
		try {
			return TemplateHandler.loadFrom("/template/smallExample.xml");
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		fail("unable to load");
		return null;
	}
	
	
    @Test
    public void simpleBytesWriteReadLowLevel() {
    	boolean useHighLevel = false;    	
    	singleFragmentWriteRead(useHighLevel);    
    }

	
    @Test
    public void simpleBytesWriteReadHighLevel() {
    	boolean useHighLevel = true;    	
    	singleFragmentWriteRead(useHighLevel);    
    }


	private void singleFragmentWriteRead(boolean useHighLevel) {
		byte primaryRingSizeInBits = 7; 
    	byte byteRingSizeInBits = 16;
    	
		RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
		ring.initBuffers();
		//Setup the test data sizes derived from the templates used
		byte[] target = new byte[ring.maxAvgVarLen];
		
		
		int LARGEST_MESSAGE_SIZE = FROM.fragDataSize[MSG_SAMPLE_LOC];    
        int testSize = ((1<<primaryRingSizeInBits)/LARGEST_MESSAGE_SIZE)-2;
        
        if (useHighLevel) {
            populateRingBufferHighLevel(ring, ring.maxAvgVarLen, testSize);
        } else {
        	populateRingBufferLowLevel(ring, ring.maxAvgVarLen, testSize);
        }
       
        //now read the data back
        int k = testSize;
        while (RingReader.tryReadFragment(ring)) {
        	if (RingReader.isNewMessage(ring)) {
        		--k;
        		int expectedLength = (ring.maxAvgVarLen*k)/testSize;	
        		
        		int msgLoc = RingReader.getMsgIdx(ring);
        		if (msgLoc<0) {
        			return;
        		}
        		
        		//must cast for this test because the id can be 64 bits but we can only switch on 32 bit numbers
        		int templateId = (int)FROM.fieldIdScript[msgLoc];
        		       		
        	//	System.err.println("read TemplateID:"+templateId);
        		switch (templateId) {
	        		case 2:
	        		//	System.err.println("checking with "+k);
	        			
	        			assertEquals(MSG_BOXES_LOC,msgLoc);
	        			
	        			int count = RingReader.readInt(ring, BOX_COUNT_LOC);
	        			assertEquals(42,count);
	        			
	        			int ownLen = RingReader.readBytes(ring, BOX_OWNER_LOC, target, 0);
	        			assertEquals(expectedLength,ownLen);

	        		//	System.err.println("BOX LOC:"+Integer.toHexString(BOX_COUNT_LOC));
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

	private void populateRingBufferHighLevel(RingBuffer ring, int blockSize, int testSize) {
		
		int[] templateIds = new int[] {2,1,4};
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		RingWriter.publishEOF(ring);
        		return;//done
        	}
        	
        	//for this test we just round robin the message types.
        	int selectedTemplateId  =  templateIds[j%templateIds.length];
        	
        	//System.err.println("write template:"+selectedTemplateId);
        	
        	switch(selectedTemplateId) {
	        	case 2: //boxes
	        		if (RingWriter.tryWriteFragment(ring, MSG_BOXES_LOC)) { //AUTO writes template id as needed
		        		j--;
		        		byte[] source = buildMockData((j*blockSize)/testSize);
		        		
		        		RingWriter.writeInt(ring, BOX_COUNT_LOC, 42);
		        		RingWriter.writeBytes(ring, BOX_OWNER_LOC, source);
	        		
		        		RingWriter.publishWrites(ring); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	            		Thread.yield();
	            		
	            	}       
	        		break;
	        	case 1: //samples
	        		if (RingWriter.tryWriteFragment(ring, MSG_SAMPLE_LOC)) { 
		        		j--;
		        				        		
		        		RingWriter.writeInt(ring, SAMPLE_YEAR_LOC ,2014);
		        		RingWriter.writeInt(ring, SAMPLE_MONTH_LOC ,12);
		        		RingWriter.writeInt(ring, SAMPLE_DATE_LOC ,9);
		        		RingWriter.writeDecimal(ring,  SAMPLE_WEIGHT, 2, (long) 123456);
		        				        		
		        		RingWriter.publishWrites(ring); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	        			Thread.yield();
	            		
	            	}  
	        		break;
	        	case 4: //reset
	        		if (RingWriter.tryWriteFragment(ring, MSG_RESET_LOC)) { 
	        			j--;
	        			
	        			RingWriter.writeBytes(ring, REST_VERSION, ASCII_VERSION);
	        			RingWriter.publishWrites(ring); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	        			Thread.yield();
	            		
	            	}  
	        		break;
        	}        	
        	
        }
	}

	private void populateRingBufferLowLevel(RingBuffer ring, int blockSize, int testSize) {
		
		int[] templateIds = new int[] {2,1,4};
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		ring.llRead.llrTailPosCache = spinBlockOnTail(ring.llRead.llrTailPosCache, RingBuffer.workingHeadPosition(ring) - (ring.maxSize - 1), ring);
        		RingBuffer.publishEOF(ring);
        		return;//done
        	}
        	
        	//for this test we just round robin the message types.
        	int selectedTemplateId  =  templateIds[j%templateIds.length];
        	
        	//System.err.println("write template:"+selectedTemplateId);
        	
        	switch(selectedTemplateId) {
	        	case 2: //boxes
	        		ring.llRead.llrTailPosCache = spinBlockOnTail(ring.llRead.llrTailPosCache, RingBuffer.workingHeadPosition(ring) - (ring.maxSize - 4), ring);
	        		
	        		j--;
	        		RingBuffer.addMsgIdx(ring, MSG_BOXES_LOC);
	        		byte[] source = buildMockData((j*blockSize)/testSize);
                RingBuffer.addIntValue(42, ring);
	        		RingBuffer.addByteArray(source, 0, source.length, ring);
	        		RingBuffer.publishWrites(ring);
	        		break;
	        	case 1: //samples
	        		ring.llRead.llrTailPosCache = spinBlockOnTail(ring.llRead.llrTailPosCache, RingBuffer.workingHeadPosition(ring) - (ring.maxSize - 8), ring);
	        		
	        		j--;
	        		RingBuffer.addMsgIdx(ring, MSG_SAMPLE_LOC);
                RingBuffer.addIntValue(2014, ring);
                RingBuffer.addIntValue(12, ring);
                RingBuffer.addIntValue(9, ring);
	        		
                RingBuffer.addIntValue(2, ring);
	        		RingBuffer.addLongValue(ring, 123456);

	        		RingBuffer.publishWrites(ring);
	        		break;
	        	case 4: //reset
	        		ring.llRead.llrTailPosCache = spinBlockOnTail(ring.llRead.llrTailPosCache, RingBuffer.workingHeadPosition(ring) - (ring.maxSize - 3), ring);
	        		
	        		j--;
	        		RingBuffer.addMsgIdx(ring, MSG_RESET_LOC);
	        		RingBuffer.addByteArray(ASCII_VERSION, 0, ASCII_VERSION.length, ring);

	        		RingBuffer.publishWrites(ring);

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
	
	/**
	 * Simple single threaded test of write and read sequence when we want to write the length of the sequence after each member.
	 */
	@Test
	public void sequenceFragmentWriteRead() {
		byte primaryRingSizeInBits = 7; 
    	byte byteRingSizeInBits = 16;
    
    	
		RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
		ring.initBuffers();
		int testSize = 5;
		
		//in this method we write two sequence members but only record the count after writing the members
		populateRingBufferWithSequence(ring, testSize);
		
		//Ring is full of messages, this loop runs until the ring is empty.
        while (RingReader.tryReadFragment(ring)) {
        	assertTrue(RingReader.isNewMessage(ring));

        	int msgIdx = RingReader.getMsgIdx(ring);
        	if (msgIdx<0) {
        		break;
        	}
			assertEquals(MSG_TRUCKS_LOC, msgIdx);

			assertEquals("TheBobSquad", RingReader.readASCII(ring, SQUAD_NAME, new StringBuilder()).toString());
			
			int sequenceCount = RingReader.readInt(ring, SQUAD_NO_MEMBERS);
			assertEquals(2,sequenceCount);
        	
					
			//now we now that we have 2 fragments to read
			RingReader.tryReadFragment(ring);
			assertEquals(10, RingReader.readLong(ring, SQUAD_TRUCK_ID));
			assertEquals(2000, RingReader.readDecimalMantissa(ring, TRUCK_CAPACITY));
			assertEquals(2, RingReader.readDecimalExponent(ring, TRUCK_CAPACITY));
			assertEquals(20.00d, RingReader.readDouble(ring, TRUCK_CAPACITY),.001);
        	
			RingReader.tryReadFragment(ring);
			assertEquals(11, RingReader.readLong(ring, SQUAD_TRUCK_ID));
			assertEquals(3000, RingReader.readDecimalMantissa(ring, TRUCK_CAPACITY));
			assertEquals(2, RingReader.readDecimalExponent(ring, TRUCK_CAPACITY));
			assertEquals(30.00d, RingReader.readDouble(ring, TRUCK_CAPACITY),.001);
        	
        }
		
	}
	
	//TODO: A, it would be nice to discover early that the ring buffer is too small for a sequence of size x, TBD
	
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
        		
        		//WRITE THE SECOND MEMBER OF THE SEQ
        		//block to ensure we have room for the next fragment, and ensure that bytes consumed gets recorded
        		RingWriter.blockWriteFragment(ring, MSG_TRUCK_SEQ_LOC);
        		
        		RingWriter.writeLong(ring, SQUAD_TRUCK_ID, 11);
        		RingWriter.writeDouble(ring, TRUCK_CAPACITY, 30d, 2); //alternate way of writing a decimal
        		
        		//NOTE: because we are waiting until the end of the  sequence to write its length we have two rules
        		//      1. Publish can not be called between these fragments because it will publish a zero for the count
        		//      2. The RingBuffer must be large enough to hold all the fragments in the sequence.
        		//      Neither one of these apply when the length can be set first.
        		
        		RingWriter.writeInt(ring, SQUAD_NO_MEMBERS, 2); //NOTE: we are writing this field very late because we now know how many we wrote.
        		
        		RingWriter.publishWrites(ring);
        		        		
        		 j--;       		
    		} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.yield();
        		
        	}     
        }
	}
	
	//TODO: B, build a unit test to show nested sequences.
	
	@Test
	public void zeroSequenceFragmentWriteRead() {
    	
		RingBuffer ring = new RingBuffer(new RingBufferConfig(FROM, 60, 60));
		ring.initBuffers();
		int testSize = 25;
		
		//in this method we write two sequence members but only record the count after writing the members
		populateRingBufferWithZeroSequence(ring, testSize);
		
		//Ring is full of messages, this loop runs until the ring is empty.
		int j = testSize;
        while (RingReader.tryReadFragment(ring)) {
        	
        	//RingReader.printFragment(ring);
        	
        	int msgIdx = RingReader.getMsgIdx(ring);
        	if (msgIdx<0) {
        		break;
        	}
			assertEquals(MSG_TRUCKS_LOC, msgIdx);

			assertEquals("TheBobSquad", RingReader.readASCII(ring, SQUAD_NAME, new StringBuilder()).toString());
			
			int sequenceCount = RingReader.readInt(ring, SQUAD_NO_MEMBERS);
			
			
			if (0==(--j&1)) {			
			    assertTrue(RingReader.isNewMessage(ring));
			    assertEquals(0,sequenceCount);
			    RingReader.tryReadFragment(ring); //WARNING: this is often missed.
			    assertFalse(RingReader.isNewMessage(ring));
			} else {
			    assertTrue(RingReader.isNewMessage(ring));
			    assertEquals(1,sequenceCount);
		         RingReader.tryReadFragment(ring);
		        // RingReader.printFragment(ring);
		         assertEquals(11, RingReader.readLong(ring, SQUAD_TRUCK_ID));
		         assertEquals(3000, RingReader.readDecimalMantissa(ring, TRUCK_CAPACITY));
		         assertEquals(2, RingReader.readDecimalExponent(ring, TRUCK_CAPACITY));
			}
			
			RingReader.releaseReadLock(ring);
		        	
        }
		
	}

	private void populateRingBufferWithZeroSequence(RingBuffer ring, int testSize) {
		
		int j = testSize;
        while (--j>=0) {
        	
        	if (RingWriter.tryWriteFragment(ring, MSG_TRUCKS_LOC)) { //AUTO writes template id as needed
 
        		RingWriter.writeASCII(ring, SQUAD_NAME, "TheBobSquad");     		
        		RingWriter.blockWriteFragment(ring, MSG_TRUCK_SEQ_LOC);                    
        		        		
        		if (0==(j&1)) {
        		    RingWriter.writeInt(ring, SQUAD_NO_MEMBERS, 0); //NOTE: we are writing this field very late because we now know how many we wrote.
        		} else {
            		
            		//block to ensure we have room for the next fragment, and ensure that bytes consumed gets recorded
                    RingWriter.writeLong(ring, SQUAD_TRUCK_ID, 11);
                    RingWriter.writeDouble(ring, TRUCK_CAPACITY, 30d, 2); //alternate way of writing a decimal
                    
                    //NOTE: because we are waiting until the end of the  sequence to write its length we have two rules
                    //      1. Publish can not be called between these fragments because it will publish a zero for the count
                    //      2. The RingBuffer must be large enough to hold all the fragments in the sequence.
                    //      Neither one of these apply when the length can be set first.
                    
                    RingWriter.writeInt(ring, SQUAD_NO_MEMBERS, 1); //NOTE: we are writing this field very late because we now know how many we wrote.
        		}
        		
        		RingWriter.publishWrites(ring);
        		RingBuffer.publishAllBatchedWrites(ring);
        		          		
    		} 
        }
        RingWriter.publishEOF(ring);
                
	}
	

	@Test // Work in progress need to get shutdown working.
    public void generatedTest() {
        
        final int testSize = 830;//81;//30000;
        int seed = 42;        
        
        RingBuffer ring = buildPopulatedRing(FROM, new RingBufferConfig(FROM, 20000, 40), seed, testSize, 40);
                        
        StringBuilder target = new StringBuilder();
        
        //Ring is full of messages, this loop runs until the ring is empty.
        int msgCount = 0;
        int fragCount = 0;
        try {
            while (RingReader.tryReadFragment(ring)) {
                 fragCount++; 
                 if (RingReader.isNewMessage(ring)) {
                     msgCount++;
                 }
    
                 target.setLength(0);
                 RingReader.printFragment(ring, target);                 
                 assertTrue(target.length()>0);
                 
                 //TODO: AA, Must add validator that values are the same as generated.
                 //TODO: AA, Must add validator that values are in the range of contract
                 
                 System.err.println(target); //TODO: A, must resolve this error.
                 
                 
                int msgIdx = RingReader.getMsgIdx(ring);
                if (msgIdx<0) {
                    System.err.println("exit early");
                    break;
                }
                
                //confirm that all message Ids are valid
                int[] starts = FROM.messageStarts;
                int i = starts.length;
                boolean found = false;
                while (--i>=0) {
                    found |= starts[i]==msgIdx;
                }
                assertTrue(found);
                
                RingReader.releaseReadLock(ring);
                    
            }
            // System.err.println("message count "+msgCount);
        } finally {
            
            if (fragCount<testSize) {
                System.err.println();
                System.err.println("Last read fragment at message count:"+msgCount+" fragmentCount:"+fragCount);
                System.err.println(target);
            }
        }
        
    }
	
	
    public RingBuffer buildPopulatedRing(FieldReferenceOffsetManager from, RingBufferConfig rbConfig, int commonSeed, int iterations, int varLength) {
        int i;
        RingBuffer ring2 = new RingBuffer(rbConfig);
        ring2.initBuffers();
        
        StreamingWriteVisitorGenerator swvg2 = new StreamingWriteVisitorGenerator(from, new Random(commonSeed), varLength, varLength);    
                
        StreamingVisitorWriter svw2 = new StreamingVisitorWriter(ring2, swvg2);
        
        svw2.startup();     
        i = iterations;
        while (--i>=0 || !svw2.isAtBreakPoint()) {
            svw2.run();
        }
        svw2.shutdown();
        
        
                
        return ring2;
    }
    
    
    
    
    
    
	
}
