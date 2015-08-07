package org.slf4j.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class RingBufferLoggerTest implements RingBufferLoggerMessageConsumer {
 	private static int[] SINGLE_MESSAGE_TOKENS = new int[]{TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_None, 0),
	   TokenBuilder.buildToken(TypeMask.LongSigned, OperatorMask.Field_None, 0),
	   };
	private static String[] SINGLE_MESSAGE_NAMES = new String[]{"Decimal"};
	private static long[] SINGLE_MESSAGE_IDS = new long[]{0};
	private static final short ZERO_PREMABLE = 0;
	private static final byte PRIMARY_RING_SIZE = 8; 
	private static final byte BYTE_RING_SIZE = 16;
	public static final FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(SINGLE_MESSAGE_TOKENS, 
	ZERO_PREMABLE, 
	SINGLE_MESSAGE_NAMES, 
	SINGLE_MESSAGE_IDS);
	private final RingBufferConfig CONFIG = new RingBufferConfig(PRIMARY_RING_SIZE,BYTE_RING_SIZE,null,FROM);
	
	private final int FRAG_LOC = 0;
	private final int FRAG_FIELD_ASC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
	private final int FRAG_FIELD = FieldReferenceOffsetManager.lookupFieldLocator(SINGLE_MESSAGE_NAMES[0], 0, FROM);
	private RingBufferLogger logger;
	private Thread loggerThread;
	private boolean stopWriting = false;


    @Ignore //Need Roman to fix this up since migrating it in
    public void testLoggingDirrectlyToSystemError() {
     	loggerThread = null;
    	initiateLogger("Output Directly to System.Error", 
    					"test1", 0, null, null);
    	writeReadDecimalsThreaded();
    	logger.stopWriting();
    }
    
    @Ignore //Need Roman to fix this up since migrating it in
    public void testLoggingToRingAndSystemError() {
    	loggerThread = null;
    	initiateLogger("Output to Ring and then to System.Error", 
    					"test2", BYTE_RING_SIZE, null, null);
    	writeReadDecimalsThreaded();
    	logger.stopWriting();
    }
    
    @Ignore //Need Roman to fix this up since migrating it in
    public void testLoggingToRingAndWritingHere() {
    	loggerThread = null;
    	initiateLogger("Output to Ring and then Writing messages here to System.out", 
    					"test3", BYTE_RING_SIZE, null, this);
    	writeReadDecimalsThreaded();
    	logger.stopWriting();
    }
    
    @Ignore //Need Roman to fix this up since migrating it in
    public void testLoggingToALocalRingAndReadingHere() {
    	RingBuffer loggerRing = new RingBuffer(new RingBufferConfig((byte)7,BYTE_RING_SIZE,null,FieldReferenceOffsetManager.RAW_BYTES));
    	loggerThread = createThreadToReadFromLoggerBuffer(loggerRing);
    	initiateLogger("Output to a local Ring and then Reading from it", 
    					"test4", 0, loggerRing, null);
    	writeReadDecimalsThreaded();
    	stopWriting = true;
    	while(loggerThread.isAlive()) {
    		Thread.yield();
    	}
    }
	private void readTestValue(RingBuffer ring, int varDataMax, int testSize,
			int FIELD_LOC, int k) {		
		
		int expectedValue = ((varDataMax*(k))/testSize);		        	
		
		int exp = RingReader.readDecimalExponent(ring, FIELD_LOC);
		long man = RingReader.readDecimalMantissa(ring, FIELD_LOC);
		
		//System.err.println("read "+exp+" and "+man);
		
		float floatValue = RingReader.readFloat(ring, FIELD_LOC);
		assertEquals(floatValue+"",2, exp);
		assertEquals(floatValue+"",expectedValue,man);
 		logger.info("                         #"+(testSize-k)+" <- "+floatValue);
	}
    
	@Ignore //Need Roman to fix this up since migrating it in
    public void testLoggingException() {
    	loggerThread = null;
    	initiateLogger("Output to Ring: messages and exceptions", 
    					"test5", 20, null, null);
    	try {
    		logger.info("Starting testLoggingException()");
    		testMethod1();
    	}
    	catch(Exception e) {
    		logger.error("Faild in the testMethod1()",e);
    	}
    	finally {
    		logger.info("Finished testLoggingException()");
        	logger.stopWriting();
    	}
    }
    
	private void writeTestValue(RingBuffer ring, int blockSize, int testSize) {
		int j = testSize;
		
        while (true) {
        	
        	if (j == 0) {
       			logger.info("#"+(testSize+1)+" COMPLETED WRITING");
				RingWriter.publishEOF(ring);
        		return;//done
        	}
        
        	if (RingWriter.tryWriteFragment(ring, FRAG_LOC)) { //returns true if there is room to write this fragment
     		
        		int value = (--j*blockSize)/testSize;

        		float floatEquivalent = (float)value / 100;
       			logger.info("#"+(testSize-j)+" -> "+floatEquivalent);
       		
        		RingWriter.writeDecimal(ring, FRAG_FIELD, 2, (long) value );
        	
        		RingWriter.publishWrites(ring); //must always publish the writes if message or fragment
        		
        	} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.yield();
        	}        	
        	
        }
	}
	   
   private void writeReadDecimalsThreaded() {
	    final RingBuffer ring = new RingBuffer(CONFIG);
    	
        final int messageSize = FROM.fragDataSize[FRAG_LOC];
        final int varDataMax = (ring.byteMask/(ring.mask>>1))/messageSize;        
        final int testSize = (1<<7)/messageSize;
    	Thread t = new Thread(new Runnable(){

			@Override
			public void run() {
				writeTestValue(ring, varDataMax, testSize);
			}}
			);
    	t.start();
        
    	if(loggerThread != null) {
    		// Starting reader from the logger buffer
    		loggerThread.start();
    	}
    	
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
	        	
	        	readTestValue(ring, varDataMax, testSize, FIELD_LOC, k);
	        	
	        } else {
	        	//unable to read so at this point
	        	//we can do other work and try again soon
	        	Thread.yield();
	        	
	        }
        }
                
   }

   private void testMethod1() throws Exception{
		logger.info("Starting method1()");
		try {
			testMethod2();
		}
		catch(Exception e) {
			throw new RuntimeException("Failed in the testMethod2()",e);
		}
		logger.info("Finished method1()");
	}
	private void testMethod2() throws Exception{
		logger.info("Starting method2()");
		int k = 10;
		int m = 0;
		int l = (int)(k / m);
		logger.info("Finished method2() l="+l);
	}
    private Thread createThreadToReadFromLoggerBuffer(final RingBuffer ring) {
	   stopWriting = false;
	   Thread t = new Thread(new Runnable(){

			@Override
			public void run() {
		        while (true) {
		        	StringBuffer target = new StringBuffer();
		        	if(RingReader.tryReadFragment(ring)) {
			        	RingReader.readASCII(ring, FRAG_FIELD_ASC, target);
		        		consumeMessage(target);
			        }
			        else if(stopWriting) {
			        	return;
			        }
			        else {
			        	//unable to read so at this point
			        	//we can do other work and try again soon
			        	Thread.yield();		        			        }
		        }
			}
		});
		return t;
   }
   /**
    * This method initiate the RingBufferLoger in one of the following modes depending on provided arguments.
    * 1) Directly To File 		(byteRingSizeInBits=0, ring=null, consumer=null). This mode is equivalent to SimpleLogger.
    * 2) To Ring, Then To File	(byteRingSizeInBits>0, ring=null, consumer=null). Logger writes messages to ring and then 
    *                                                                    consume them and writes to a file.
    * 3) To Ring, Then to Consumer (byteRingSizeInBits>0, ring=null. consumer=this). Logger writes messages to ring and then 
    *                                                                    read them and delegate to the external consumer.
    * 4) To external ring (byteRingSizeInBits=0, ring!=null, consumer=null). Logger just write messages to the provided ring.
    * 																	It is ring owner responsibility to consume messages.
    * @param title Title to indicate test and separate outputs.
    * @param name Logger name. Each test must have different name.
    * @param byteRingSizeInBits int size of the byte ring of the logger in bits. If not 0 then  creating a logger ring.
    * @param ring external RingBuffer
    * @param consumer consumer of the log messages
    */
   private void initiateLogger(String title, String name, 
		   int byteRingSizeInBits, RingBuffer ring,
		   RingBufferLoggerMessageConsumer consumer) {
   	System.setProperty(RingBufferLogger.BYTE_RING_SIZE,""+byteRingSizeInBits);
   	logger = (RingBufferLogger)StaticLoggerBinder.getSingleton()
				  .getLoggerFactory()
				  .getLogger(name);
   	if(consumer != null) {
   		logger.setConsumer(consumer);
   	}
   	if(ring!=null) {
   		logger.setConsumer(null);
   		logger.setRingBuffer(ring);
   	}
   	StringBuilder buf = new StringBuilder();
   	for(int i=0;i<title.length();i++) buf.append('-');
   	logger.info(buf.toString());
   	logger.info(title);
   	logger.info(buf.toString());
   }

	@Override
	public void consumeMessage(StringBuffer message) {
		message.append(" |");
		System.out.println(message);
		
	}
}
