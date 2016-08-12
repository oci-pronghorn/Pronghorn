package com.ociweb.pronghorn.pipe;

import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupFragmentLocator;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupTemplateLocator;
import static com.ociweb.pronghorn.pipe.Pipe.spinBlockOnTail;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.pipe.stream.StreamingWriteVisitorGenerator;

public class PipeMultiTemplateTest {

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
    public void simpleBytesWriteLowLevelReadHighLevel() {
    	boolean useHighLevel = false;    	
    	singleFragmentReadHighLevel(useHighLevel);    
    }

	
    @Test
    public void simpleBytesWriteHighLevelReadHighLevel() {
    	boolean useHighLevel = true;    	
    	singleFragmentReadHighLevel(useHighLevel);    
    }


    @Test
    public void simpleBytesWriteLowLevelReadLowLevel() {
    	boolean useHighLevel = false;    	
    	singleFragmentReadLowLevel(useHighLevel);    
    }

	
    @Test
    public void simpleBytesWriteHighLevelReadLowLevel() {
    	boolean useHighLevel = true;    	
    	singleFragmentReadLowLevel(useHighLevel);    
    }


	private void singleFragmentReadHighLevel(boolean useHighLevelToPopulate) {
		byte primaryRingSizeInBits = 7; 
    	byte byteRingSizeInBits = 16;
    	
    	boolean testReplayFeature = true;
    	
		Pipe<RawDataSchema> ring = new Pipe<RawDataSchema>(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null, new MessageSchemaDynamic(FROM)));
		ring.initBuffers();
		//Setup the test data sizes derived from the templates used
		byte[] target = new byte[ring.maxAvgVarLen];
		
		
		int LARGEST_MESSAGE_SIZE = FROM.fragDataSize[MSG_SAMPLE_LOC];    
        int testSize = ((1<<primaryRingSizeInBits)/LARGEST_MESSAGE_SIZE)-2;
        
        if (useHighLevelToPopulate) {
            populateRingBufferHighLevel(ring, ring.maxAvgVarLen, testSize);
        } else {
        	populateRingBufferLowLevel(ring, ring.maxAvgVarLen, testSize);
        }
       
        //now read the data back
        int k = testSize;
        while (PipeReader.tryReadFragment(ring)) {
        	if (PipeReader.isNewMessage(ring)) {
        		--k;
        		int expectedLength = (ring.maxAvgVarLen*k)/testSize;	
        		
        		int msgLoc = PipeReader.getMsgIdx(ring);
        		if (msgLoc<0) {
        			return;
        		}
        		
        		//must cast for this test because the id can be 64 bits but we can only switch on 32 bit numbers
        		int templateId = (int)FROM.fieldIdScript[msgLoc];
        		       		
        		switch (templateId) {
	        		case 2:
	        			
	        			assertEquals(MSG_BOXES_LOC,msgLoc);
	        			
	        			int count = PipeReader.readInt(ring, BOX_COUNT_LOC);
	        			assertEquals(42,count);
	        			
	        			int ownLen = PipeReader.readBytes(ring, BOX_OWNER_LOC, target, 0);
	        			assertEquals(expectedLength,ownLen);

	        			break;
	        		case 1:
	        			assertEquals(MSG_SAMPLE_LOC,msgLoc);
	        			
	        			int year = PipeReader.readInt(ring, SAMPLE_YEAR_LOC);
	        			assertEquals(2014,year);
	        			
	        			int month = PipeReader.readInt(ring, SAMPLE_MONTH_LOC);
	        			assertEquals(12,month);
	        			
	        			int day = PipeReader.readInt(ring, SAMPLE_DATE_LOC);
	        			assertEquals(9,day);
	        			
	        			long wMan = PipeReader.readDecimalMantissa(ring, SAMPLE_WEIGHT);
	        			assertEquals(123456,wMan);
	        			
	        			int wExp = PipeReader.readDecimalExponent(ring, SAMPLE_WEIGHT);
	        			assertEquals(2,wExp);	        			
	        			
	        			break;
	        		case 4:
	        			assertEquals(MSG_RESET_LOC,msgLoc);
	        			int verLen = PipeReader.readBytes(ring, REST_VERSION, target, 0);
	        			assertEquals(3,verLen);	
	        			
	        			break;
	        		default:
	        			fail("Unexpected templateId of "+templateId);
	        			break;
        		
        		}       		
        	} else {
        		fail("All fragments are messages for this test.");
        	}
        	
        	if (testReplayFeature) {
        		
        		Pipe.replayUnReleased(ring);
        		Pipe.cancelReplay(ring);
        	}
        	
        }
	}

		
	private void singleFragmentReadLowLevel(boolean useHighLevelToPopulate) {
		byte primaryRingSizeInBits = 7; 
    	byte byteRingSizeInBits = 16;
    	
    	boolean testReplayFeature = true;
    	
		Pipe<RawDataSchema> ring = new Pipe<RawDataSchema>(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null, new MessageSchemaDynamic(FROM)));
		ring.initBuffers();
		//Setup the test data sizes derived from the templates used
		byte[] target = new byte[ring.maxAvgVarLen];
		
		
		int LARGEST_MESSAGE_SIZE = FROM.fragDataSize[MSG_SAMPLE_LOC];    
        int testSize = ((1<<primaryRingSizeInBits)/LARGEST_MESSAGE_SIZE)-2;
        
        if (useHighLevelToPopulate) {
            populateRingBufferHighLevel(ring, ring.maxAvgVarLen, testSize);
        } else {
        	populateRingBufferLowLevel(ring, ring.maxAvgVarLen, testSize);
        }
       
        //now read the data back
        int k = testSize;
                
        //Prevent this from release until we are ready
        Pipe.setReleaseBatchSize(ring, 8);//Integer.MAX_VALUE);
        
        
        while (Pipe.hasContentToRead(ring, 1)) {
        	        	
    		--k;
    		int expectedLength = (ring.maxAvgVarLen*k)/testSize;	
    		
    		final int msgLoc =  Pipe.takeMsgIdx(ring);
    		if (msgLoc<0) {
    			return;
    		}
    		
    		//must cast for this test because the id can be 64 bits but we can only switch on 32 bit numbers
    		int templateId = (int)FROM.fieldIdScript[msgLoc];
    		       		
    		switch (templateId) {
        		case 2:
        			
        			assertEquals(MSG_BOXES_LOC,msgLoc);
        			
        			int count = Pipe.takeValue(ring);
        			assertEquals(42,count);
        				        
        			{
        				int meta = Pipe.takeRingByteMetaData(ring);
        	        	int len = Pipe.takeRingByteLen(ring);
        	        	assertEquals(expectedLength, len);
        	        	Pipe.readBytes(ring, target, 0, 0xFFFFFFFF, meta, len);	        	        
        			}

        			break;
        		case 1:
        			assertEquals(MSG_SAMPLE_LOC,msgLoc);
        			
        			int year = Pipe.takeValue(ring);
        			assertEquals(2014,year);
        			
        			int month = Pipe.takeValue(ring);
        			assertEquals(12,month);
        			
        			int day = Pipe.takeValue(ring);
        			assertEquals(9,day);
        			
        			
        			int wExp = Pipe.takeValue(ring);
        			assertEquals(2,wExp);	        			
        			
        			long wMan = Pipe.takeLong(ring);
        			assertEquals(123456,wMan);
        			
        			
        			break;
        		case 4:
        			assertEquals(MSG_RESET_LOC,msgLoc);
        			
        			{
        				int meta = Pipe.takeRingByteMetaData(ring);
        	        	int len = Pipe.takeRingByteLen(ring);
        	        	assertEquals(3,len);
        	        	Pipe.readBytes(ring, target, 0, 0xFFFFFFFF, meta, len);	   
        			}
        			
        			break;
        		default:
        			fail("Unexpected templateId of "+templateId);
        			break;    		
    		}       		
        	
    		Pipe.releaseReads(ring);
    
    		Pipe.confirmLowLevelRead(ring, FROM.fragDataSize[msgLoc]);
        		
        	if (testReplayFeature) {
        		
        		Pipe.replayUnReleased(ring);
        		
        		assertTrue(Pipe.isReplaying(ring));
        		
        		//we are now ready to re-read the last message
        		int reReadMsgIdx = Pipe.takeMsgIdx(ring);
        		assertEquals(msgLoc,reReadMsgIdx);
        		        		
        		Pipe.cancelReplay(ring);
        	}
        	
        	Pipe.releaseAllBatchedReads(ring);
        	
        }
	}
	
	
	private void populateRingBufferHighLevel(Pipe<RawDataSchema> pipe, int blockSize, int testSize) {
		
		int[] templateIds = new int[] {2,1,4};
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		PipeWriter.publishEOF(pipe);
        		return;//done
        	}
        	
        	//for this test we just round robin the message types.
        	int selectedTemplateId  =  templateIds[j%templateIds.length];
        	
        	//System.err.println("write template:"+selectedTemplateId);
        	
        	switch(selectedTemplateId) {
	        	case 2: //boxes
	        		if (PipeWriter.tryWriteFragment(pipe, MSG_BOXES_LOC)) { //AUTO writes template id as needed
		        		j--;
		        		byte[] source = buildMockData((j*blockSize)/testSize);
		        		
		        		PipeWriter.writeInt(pipe, BOX_COUNT_LOC, 42);
		        		PipeWriter.writeBytes(pipe, BOX_OWNER_LOC, source);
	        		
		        		PipeWriter.publishWrites(pipe); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	            		Thread.yield();
	            		
	            	}       
	        		break;
	        	case 1: //samples
	        		if (PipeWriter.tryWriteFragment(pipe, MSG_SAMPLE_LOC)) { 
		        		j--;
		        				        		
		        		PipeWriter.writeInt(pipe, SAMPLE_YEAR_LOC ,2014);
		        		PipeWriter.writeInt(pipe, SAMPLE_MONTH_LOC ,12);
		        		PipeWriter.writeInt(pipe, SAMPLE_DATE_LOC ,9);
		        		PipeWriter.writeDecimal(pipe,  SAMPLE_WEIGHT, 2, (long) 123456);
		        				        		
		        		PipeWriter.publishWrites(pipe); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	        			Thread.yield();
	            		
	            	}  
	        		break;
	        	case 4: //reset
	        		if (PipeWriter.tryWriteFragment(pipe, MSG_RESET_LOC)) { 
	        			j--;
	        			
	        			PipeWriter.writeBytes(pipe, REST_VERSION, ASCII_VERSION);
	        			PipeWriter.publishWrites(pipe); //must always publish the writes if message or fragment
	        		} else {
	            		//Unable to write because there is no room so do something else while we are waiting.
	        			Thread.yield();
	            		
	            	}  
	        		break;
        	}        	
        	
        }
	}

	private void populateRingBufferLowLevel(Pipe<RawDataSchema> pipe, int blockSize, int testSize) {
		
		int[] templateIds = new int[] {2,1,4};
		int j = testSize;
        while (true) {
        	
        	if (j == 0) {
        		pipe.llRead.llrTailPosCache = spinBlockOnTail(pipe.llRead.llrTailPosCache, Pipe.workingHeadPosition(pipe) - (pipe.sizeOfSlabRing - 1), pipe);
        		Pipe.publishEOF(pipe);
        		return;//done
        	}
        	
        	//for this test we just round robin the message types.
        	int selectedTemplateId  =  templateIds[j%templateIds.length];
        	
        	//System.err.println("write template:"+selectedTemplateId);
        	
        	switch(selectedTemplateId) {
	        	case 2: //boxes
	        		pipe.llRead.llrTailPosCache = spinBlockOnTail(pipe.llRead.llrTailPosCache, Pipe.workingHeadPosition(pipe) - (pipe.sizeOfSlabRing - 4), pipe);
	        		
	        		j--;
	        		Pipe.addMsgIdx(pipe, MSG_BOXES_LOC);
	        		byte[] source = buildMockData((j*blockSize)/testSize);
                Pipe.addIntValue(42, pipe);
	        		Pipe.addByteArray(source, 0, source.length, pipe);
	        		Pipe.publishWrites(pipe);
	        		break;
	        	case 1: //samples
	        		pipe.llRead.llrTailPosCache = spinBlockOnTail(pipe.llRead.llrTailPosCache, Pipe.workingHeadPosition(pipe) - (pipe.sizeOfSlabRing - 8), pipe);
	        		
	        		j--;
	        		Pipe.addMsgIdx(pipe, MSG_SAMPLE_LOC);
                Pipe.addIntValue(2014, pipe);
                Pipe.addIntValue(12, pipe);
                Pipe.addIntValue(9, pipe);
	        		
                Pipe.addIntValue(2, pipe);
	        		Pipe.addLongValue(pipe, 123456);

	        		Pipe.publishWrites(pipe);
	        		break;
	        	case 4: //reset
	        		pipe.llRead.llrTailPosCache = spinBlockOnTail(pipe.llRead.llrTailPosCache, Pipe.workingHeadPosition(pipe) - (pipe.sizeOfSlabRing - 3), pipe);
	        		
	        		j--;
	        		Pipe.addMsgIdx(pipe, MSG_RESET_LOC);
	        		Pipe.addByteArray(ASCII_VERSION, 0, ASCII_VERSION.length, pipe);

	        		Pipe.publishWrites(pipe);

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
    
    	
		Pipe<MessageSchemaDynamic> ring = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(primaryRingSizeInBits, byteRingSizeInBits, null, new MessageSchemaDynamic(FROM)));
		ring.initBuffers();
		int testSize = 5;
		
		//in this method we write two sequence members but only record the count after writing the members
		populateRingBufferWithSequence(ring, testSize);
		
		//Ring is full of messages, this loop runs until the ring is empty.
        while (PipeReader.tryReadFragment(ring)) {
        	assertTrue(PipeReader.isNewMessage(ring));

        	int msgIdx = PipeReader.getMsgIdx(ring);
        	if (msgIdx<0) {
        		break;
        	}
			assertEquals(MSG_TRUCKS_LOC, msgIdx);

			assertEquals("TheBobSquad", PipeReader.readASCII(ring, SQUAD_NAME, new StringBuilder()).toString());
			
			int sequenceCount = PipeReader.readInt(ring, SQUAD_NO_MEMBERS);
			assertEquals(2,sequenceCount);
        	
					
			//now we now that we have 2 fragments to read
			PipeReader.tryReadFragment(ring);
			assertEquals(10, PipeReader.readLong(ring, SQUAD_TRUCK_ID));
			assertEquals(2000, PipeReader.readDecimalMantissa(ring, TRUCK_CAPACITY));
			assertEquals(2, PipeReader.readDecimalExponent(ring, TRUCK_CAPACITY));
			assertEquals(20.00d, PipeReader.readDouble(ring, TRUCK_CAPACITY),.001);
        	
			PipeReader.tryReadFragment(ring);
			assertEquals(11, PipeReader.readLong(ring, SQUAD_TRUCK_ID));
			assertEquals(3000, PipeReader.readDecimalMantissa(ring, TRUCK_CAPACITY));
			assertEquals(2, PipeReader.readDecimalExponent(ring, TRUCK_CAPACITY));
			assertEquals(30.00d, PipeReader.readDouble(ring, TRUCK_CAPACITY),.001);
        	
        }
		
	}
	
	//TODO:B, it would be nice to discover early that the ring buffer is too small for a sequence of size x, TBD
	
	private void populateRingBufferWithSequence(Pipe pipe, int testSize) {
		
		int j = testSize;
        while (true) {
        	
        	if (j==0) {
        		PipeWriter.publishEOF(pipe);
        		return;//done
        	}
        	
        	
        	if (PipeWriter.tryWriteFragment(pipe, MSG_TRUCKS_LOC)) { //AUTO writes template id as needed
 
        		PipeWriter.writeASCII(pipe, SQUAD_NAME, "TheBobSquad");     		
        		
        		//WRITE THE FIRST MEMBER OF THE SEQ
        		//block to ensure we have room for the next fragment, and ensure that bytes consumed gets recorded
        		PipeWriter.blockWriteFragment(pipe, MSG_TRUCK_SEQ_LOC);//could use tryWrite here but it would make this example more complex
        		
        		PipeWriter.writeLong(pipe, SQUAD_TRUCK_ID, 10);         
        		PipeWriter.writeDecimal(pipe, TRUCK_CAPACITY, 2, 2000);
        		
        		//WRITE THE SECOND MEMBER OF THE SEQ
        		//block to ensure we have room for the next fragment, and ensure that bytes consumed gets recorded
        		PipeWriter.blockWriteFragment(pipe, MSG_TRUCK_SEQ_LOC);
        		
        		PipeWriter.writeLong(pipe, SQUAD_TRUCK_ID, 11);
        		PipeWriter.writeDouble(pipe, TRUCK_CAPACITY, 30d, 2); //alternate way of writing a decimal
        		
        		//NOTE: because we are waiting until the end of the  sequence to write its length we have two rules
        		//      1. Publish can not be called between these fragments because it will publish a zero for the count
        		//      2. The RingBuffer must be large enough to hold all the fragments in the sequence.
        		//      Neither one of these apply when the length can be set first.
        		
        		PipeWriter.writeInt(pipe, SQUAD_NO_MEMBERS, 2); //NOTE: we are writing this field very late because we now know how many we wrote.
        		
        		PipeWriter.publishWrites(pipe);
        		        		
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
    	
		Pipe<RawDataSchema> ring = new Pipe<RawDataSchema>(new PipeConfig(new MessageSchemaDynamic(FROM), 60, 60));
		ring.initBuffers();
		int testSize = 25;
		
		//in this method we write two sequence members but only record the count after writing the members
		populateRingBufferWithZeroSequence(ring, testSize);
		
		//Ring is full of messages, this loop runs until the ring is empty.
		int j = testSize;
        while (PipeReader.tryReadFragment(ring)) {
        	
        	//RingReader.printFragment(ring);
        	
        	int msgIdx = PipeReader.getMsgIdx(ring);
        	if (msgIdx<0) {
        		break;
        	}
			assertEquals(MSG_TRUCKS_LOC, msgIdx);

			assertEquals("TheBobSquad", PipeReader.readASCII(ring, SQUAD_NAME, new StringBuilder()).toString());
			
			int sequenceCount = PipeReader.readInt(ring, SQUAD_NO_MEMBERS);
			
			
			if (0==(--j&1)) {			
			    assertTrue(PipeReader.isNewMessage(ring));
			    assertEquals(0,sequenceCount);
			    PipeReader.tryReadFragment(ring); //WARNING: this is often missed.
			    assertFalse(PipeReader.isNewMessage(ring));
			} else {
			    assertTrue(PipeReader.isNewMessage(ring));
			    assertEquals(1,sequenceCount);
		         PipeReader.tryReadFragment(ring);
		        // RingReader.printFragment(ring);
		         assertEquals(11, PipeReader.readLong(ring, SQUAD_TRUCK_ID));
		         assertEquals(3000, PipeReader.readDecimalMantissa(ring, TRUCK_CAPACITY));
		         assertEquals(2, PipeReader.readDecimalExponent(ring, TRUCK_CAPACITY));

			}
			
			PipeReader.releaseReadLock(ring);
		        	
        }
		
	}

	private void populateRingBufferWithZeroSequence(Pipe<RawDataSchema> pipe, int testSize) {
		
		int j = testSize;
        while (--j>=0) {
        	
        	if (PipeWriter.tryWriteFragment(pipe, MSG_TRUCKS_LOC)) { //AUTO writes template id as needed
 
        		PipeWriter.writeASCII(pipe, SQUAD_NAME, "TheBobSquad");     		
        		PipeWriter.blockWriteFragment(pipe, MSG_TRUCK_SEQ_LOC);                    
        		        		
        		if (0==(j&1)) {
        		    PipeWriter.writeInt(pipe, SQUAD_NO_MEMBERS, 0); //NOTE: we are writing this field very late because we now know how many we wrote.
        		} else {
            		
            		//block to ensure we have room for the next fragment, and ensure that bytes consumed gets recorded
                    PipeWriter.writeLong(pipe, SQUAD_TRUCK_ID, 11);
                    PipeWriter.writeDouble(pipe, TRUCK_CAPACITY, 30d, 2); //alternate way of writing a decimal
                    
                    //NOTE: because we are waiting until the end of the  sequence to write its length we have two rules
                    //      1. Publish can not be called between these fragments because it will publish a zero for the count
                    //      2. The RingBuffer must be large enough to hold all the fragments in the sequence.
                    //      Neither one of these apply when the length can be set first.
                    
                    PipeWriter.writeInt(pipe, SQUAD_NO_MEMBERS, 1); //NOTE: we are writing this field very late because we now know how many we wrote.
        		}
        		
        		PipeWriter.publishWrites(pipe);
        		Pipe.publishAllBatchedWrites(pipe);
        		          		
    		} 
        }
        PipeWriter.publishEOF(pipe);
                
	}
	

	@Test
    public void generatedTest() {
        
        final int testSize = 830;//81;//30000;
        int seed = 42;        
        
        Pipe ring = buildPopulatedRing(FROM, new PipeConfig(new MessageSchemaDynamic(FROM), 20000, 40), seed, testSize, 40);
                        
        StringBuilder target = new StringBuilder();
        
        //Ring is full of messages, this loop runs until the ring is empty.
        int msgCount = 0;
        int fragCount = 0;
        try {
            while (PipeReader.tryReadFragment(ring)) {
                 fragCount++; 
                 if (PipeReader.isNewMessage(ring)) {
                     msgCount++;
                 }
    
                 target.setLength(0);
                 PipeReader.printFragment(ring, target);                 
                 assertTrue(target.length()>0);
                 
                 //TODO:M, Must add validator that values are the same as generated.
                 //TODO:M, Must add validator that values are in the range of contract
                                  
                 
                int msgIdx = PipeReader.getMsgIdx(ring);
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
                
                PipeReader.releaseReadLock(ring);
                    
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
	
	
    public Pipe buildPopulatedRing(FieldReferenceOffsetManager from, PipeConfig rbConfig, int commonSeed, int iterations, int varLength) {
        int i;
        Pipe ring2 = new Pipe(rbConfig);
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
