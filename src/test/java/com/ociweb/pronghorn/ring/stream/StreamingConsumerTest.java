package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFragmentLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;

public class StreamingConsumerTest {

	private static final byte[] ASCII_VERSION = "1.0".getBytes();
    private final byte primaryRingSizeInBits = 9; 
    private final byte byteRingSizeInBits = 18;

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
	
	//closing fragment starts with the same name as the first field of that fragment
	private final int FRAG_JOMQ_LOC = lookupFragmentLocator("JustOneMoreQuestion", MSG_TRUCKS_LOC, FROM);
	private final int JOMQ_LOC = lookupFieldLocator("JustOneMoreQuestion", MSG_TRUCK_THING_SEQ_LOC, FROM);
	
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
		return null;
		
	}
	
	@Test
	public void sequenceFragmentWriteRead() {
    
    	
		RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
		ring.initBuffers();
		int testSize = 5;
		
		//in this method we write two sequence members but only record the count after writing the members
		populateRingBufferWithSequence(ring, testSize);
				
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		
		
		StreamingReadVisitor visitor = new StreamingReadVisitorToJSON(ps); 
		
		StreamingVisitorReader reader = new StreamingVisitorReader(ring, visitor);// new StreamingReadVisitorDebugDelegate(visitor) );
		
		//ring is fully populated so we should not need to call this run again
		while (RingBuffer.contentRemaining(ring)>0) {
			reader.run();
		}
		
		ps.close();
		String results = new String(baos.toByteArray());
		//spot check the produced JSON
		assertTrue(results, results.indexOf("\"TruckId\":10")>0);
		assertTrue(results, results.indexOf("{\"AThing\":7}")>0);
		assertTrue(results, results.indexOf("{\"JustOneMoreQuestion\":42}")>0);
		
	}
	
	@Test
	public void generatorTest() {
	    final int seed = 2;
	    final long aLongValue = 2945688134060370505l;//hard coded value that comes from this seed 2
	    final int aIntValue = 248789492;//hard coded value that comes from this seed 2
	    final int aNegIntValue = -51;//hard coded value that comes from this seed 2
        
	    
	       RingBuffer ring = new RingBuffer(new RingBufferConfig(FROM, 50, 30));
	       ring.initBuffers();
	       
	       
	       StreamingWriteVisitorGenerator swvg = new StreamingWriteVisitorGenerator(FROM, new Random(seed), 30, 30);
	       
	       StreamingVisitorWriter svw = new StreamingVisitorWriter(ring, swvg);
	       	       	       
	       ByteArrayOutputStream baos = new ByteArrayOutputStream();
	       PrintStream ps = new PrintStream(baos);
//	       PrintStream ps = System.out;
	       StreamingReadVisitor visitor = new StreamingReadVisitorToJSON(ps); 
	       
	       StreamingVisitorReader reader = new StreamingVisitorReader(ring, visitor );
	        
	       svw.startup();
	       reader.startup();

	       svw.run();
	       
	       reader.run();
	       
	       svw.shutdown();
	       reader.shutdown(); 	    
	    
	       String results = new String(baos.toByteArray());
	       //spot check the produced JSON
	       assertTrue(results, results.indexOf("\"Trucks\":")>0);
	       assertTrue(results, results.indexOf("{\"Squad\":")>0);
	       
	       assertTrue(results, results.indexOf(Long.toString(aLongValue))>0);
	       assertTrue(results, results.indexOf(Integer.toString(aIntValue))>0);
	       assertTrue(results, results.indexOf(Integer.toString(aNegIntValue))>0);
	}
	
//	@Test
	public void matchingTestPositive() {
	    
        RingBuffer ring1 = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        RingBuffer ring2 = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        
        ring1.initBuffers();
        ring2.initBuffers();
        
        int commonSeed = 100;         
        
        StreamingWriteVisitorGenerator swvg1 = new StreamingWriteVisitorGenerator(FROM, new Random(commonSeed), 30, 30);        
        StreamingVisitorWriter svw1 = new StreamingVisitorWriter(ring1, swvg1);
        
        StreamingWriteVisitorGenerator swvg2 = new StreamingWriteVisitorGenerator(FROM, new Random(commonSeed), 30, 30);        
        StreamingVisitorWriter svw2 = new StreamingVisitorWriter(ring2, swvg2);
        
        
        svw1.startup();
        svw2.startup();
	    
        svw1.run();
        svw2.run();
        
        svw1.run();
        svw2.run();
        
        //confirm that both rings contain the exact same thing
        assertTrue(Arrays.equals(ring1.buffer, ring2.buffer));
        assertTrue(Arrays.equals(ring1.byteBuffer, ring2.byteBuffer));
        
        //now use matcher to confirm the same.
        StreamingReadVisitorMatcher srvm = new StreamingReadVisitorMatcher(ring1);
        StreamingVisitorReader svr = new StreamingVisitorReader(ring2, srvm);// new StreamingReadVisitorDebugDelegate(srvm) );
        
        svr.startup();
        
        try {
            svr.run();
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t.getMessage());
        }
        
        svr.shutdown();
        
        svw1.shutdown();
        svw2.shutdown();
	    
	}
	
    @Test
    public void matchingTestNegative() {
        
        RingBuffer ring1 = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        RingBuffer ring2 = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        
        ring1.initBuffers();
        ring2.initBuffers();
        
        int commonSeed = 300;         
        
        StreamingWriteVisitorGenerator swvg1 = new StreamingWriteVisitorGenerator(FROM, new Random(commonSeed), 30, 30);        
        StreamingVisitorWriter svw1 = new StreamingVisitorWriter(ring1, swvg1);
        
        StreamingWriteVisitorGenerator swvg2 = new StreamingWriteVisitorGenerator(FROM, new Random(commonSeed+1), 30, 30);        
        StreamingVisitorWriter svw2 = new StreamingVisitorWriter(ring2, swvg2);
        
        
        svw1.startup();
        svw2.startup();
        
        svw1.run();
        svw2.run();
        
        svw1.run();
        svw2.run();
        
        
        StreamingReadVisitorMatcher srvm = new StreamingReadVisitorMatcher(ring1);
        StreamingVisitorReader svr = new StreamingVisitorReader(ring2, srvm);
        
        svr.startup();
        
        try {
            svr.run();
            fail("expected exception");
        } catch (Throwable t) {
            //success
            //t.printStackTrace();
        }
        
        svr.shutdown();
        
        svw1.shutdown();
        svw2.shutdown();
        
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
        		
        		RingWriter.blockWriteFragment(ring, FRAG_JOMQ_LOC);
       		
        		RingWriter.writeInt(ring, JOMQ_LOC, 42);
        		
        		RingWriter.publishWrites(ring);
        		        		        		
        		 j--;       		
    		} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.yield();
        		
        	}     
        }
	}
	
}
