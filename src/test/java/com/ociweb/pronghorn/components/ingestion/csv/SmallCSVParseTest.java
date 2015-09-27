package com.ociweb.pronghorn.components.ingestion.csv;

import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupTemplateLocator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.stream.ByteVisitor;
import com.ociweb.pronghorn.pipe.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.stream.ToOutputStreamStage;

public class SmallCSVParseTest {

	public static final boolean fileSpecificTestValues = true;
	private static final String TEST_FILE = "/small.csv";	
	
	static ByteBuffer sourceBuffer;
	static PipeConfig linesRingConfig;
	static PipeConfig fieldsRingConfig;
	static PipeConfig fieldsRingConfig2;
	static PipeConfig flatFileRingConfig;
	
	final int MSG_DOUBLE_LOC = lookupTemplateLocator("Decimal", MetaMessageDefs.FROM);  
	final int DOUBLE_VALUE_LOC = lookupFieldLocator("Value", MSG_DOUBLE_LOC,  MetaMessageDefs.FROM);
	
	final int MSG_UINT32_LOC = lookupTemplateLocator("UInt32", MetaMessageDefs.FROM);  
	final int UINT32_VALUE_LOC = lookupFieldLocator("Value", MSG_UINT32_LOC,  MetaMessageDefs.FROM);
	
	final int MSG_UINT64_LOC = lookupTemplateLocator("UInt64", MetaMessageDefs.FROM);  
	final int UINT64_VALUE_LOC = lookupFieldLocator("Value", MSG_UINT64_LOC,  MetaMessageDefs.FROM);
	
	final int MSG_INT32_LOC = lookupTemplateLocator("Int32", MetaMessageDefs.FROM);  
	final int INT32_VALUE_LOC = lookupFieldLocator("Value", MSG_INT32_LOC,  MetaMessageDefs.FROM);
	
	final int MSG_INT64_LOC = lookupTemplateLocator("Int64", MetaMessageDefs.FROM);  
	final int INT64_VALUE_LOC = lookupFieldLocator("Value", MSG_INT64_LOC,  MetaMessageDefs.FROM);
	
	final int MSG_ASCII_LOC = lookupTemplateLocator("ASCII", MetaMessageDefs.FROM);  
	final int ASCII_VALUE_LOC = lookupFieldLocator("Value", MSG_ASCII_LOC,  MetaMessageDefs.FROM);
	
	@BeforeClass
	public static void setup() {
		
		InputStream testStream = SmallCSVParseTest.class.getResourceAsStream(TEST_FILE);
		if (null==testStream) {
			try {
				testStream = new FileInputStream(TEST_FILE);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				fail();
			}
		}
				
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
	    try {
	    	byte[] buffer = new byte[1024];
	    	int len = 0;	    	
			while((len = testStream.read(buffer)) != -1){
				// assemble to the a buffer 
				baos.write(buffer, 0, len);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    sourceBuffer = ByteBuffer.wrap(baos.toByteArray());
				
	    linesRingConfig = new PipeConfig((byte)7,(byte)20,null, FieldReferenceOffsetManager.RAW_BYTES);
	    fieldsRingConfig = new PipeConfig((byte)9,(byte)19,null, MetaMessageDefs.FROM);
	    fieldsRingConfig2 = new PipeConfig((byte)10,(byte)20,null, MetaMessageDefs.FROM);
	    flatFileRingConfig = new PipeConfig((byte)14,(byte)22,null, FieldReferenceOffsetManager.RAW_BYTES);
	    
	}
	
	
	@Test
	public void testTestData() {
		 
		assertNotNull(sourceBuffer);
		assertTrue("TestFile size:"+sourceBuffer.remaining(), sourceBuffer.remaining()>100);
	}
	
	@Test
	public void testLineReader() {
		ByteBuffer data = sourceBuffer.asReadOnlyBuffer();				
		Pipe linesRing = new Pipe(linesRingConfig);
		
		//start near the end to force the rollover to happen.
		long start = linesRing.sizeOfSlabRing-15;
		int startBytes = linesRing.sizeOfBlobRing-15;
		
		linesRing.initBuffers();
		
		linesRing.reset((int)start, startBytes);
		
//		DELETE THIS BLOCK AFTER THE TESTS PASS
//		linesRing.headPos.set(start);
//		linesRing.tailPos.set(start);
//		linesRing.workingHeadPos.value=linesRing.workingTailPos.value=start;
//		
//		linesRing.bytesHeadPos.set(startBytes);
//		linesRing.bytesTailPos.set(startBytes);
//		linesRing.byteWorkingHeadPos.value=linesRing.byteWorkingTailPos.value=startBytes;
					
		GraphManager gm = new GraphManager();
		
		LineSplitterByteBufferStage lineSplitter = new LineSplitterByteBufferStage(gm, data, linesRing);
		
		//this assumes that the ring buffer is large enough to hold the small test file
		lineSplitter.startup();
		lineSplitter.run();
		lineSplitter.shutdown();
		
		RingStreams.writeEOF(linesRing);//Hack because visit bytes reqires posion pill, TODO: A, review if visit bytes can be upgraded.
		RingStreams.visitBytes(linesRing, buildLineTestingVisitor());
		
	}
	
	@Test
	public void testLineReaderRollover() {
		//Tests many different primary ring sizes to force rollover at different points.
		//Checks that every run produces the same results as the previous run.
				
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		byte[] last = null;
		
		ByteBuffer data = generateCVSData(21);		    
		int dataSize = data.limit();
		
		
		int t = 10;
		while (--t>=4) {
			data.position(0);
			data.limit(dataSize);
			
			PipeConfig linesRingConfigLocal = new PipeConfig((byte)t,(byte)20,null, FieldReferenceOffsetManager.RAW_BYTES);	
			
			final Pipe linesRing = new Pipe(linesRingConfigLocal);		
			GraphManager gm = new GraphManager();
			LineSplitterByteBufferStage lineSplitter = new LineSplitterByteBufferStage(gm, data, linesRing);
			
			baos.reset();
			ToOutputStreamStage reader = new ToOutputStreamStage(gm,linesRing,baos, false);
			
			StageScheduler ss = new ThreadPerStageScheduler(gm);
			ss.startup();
			
			ss.awaitTermination(1, TimeUnit.MINUTES);
			
			//return the position to the beginning to run the test again
			
			byte[] results = baos.toByteArray();
			if (null!=last) {
				int j = 0;
				while (j<last.length && j<results.length) {
					if (last[j]!=results[j]) {
						System.err.println("missmatch found at:"+j);						
						break;
					}
					j++;
				}		
				assertEquals(last.length,results.length);
				assertTrue("Missed on "+t+" vs "+(t+1)+" at idx"+mismatchAt(last,results)+"\n",
						   Arrays.equals(last,  results));
			}
						
			last = Arrays.copyOf(results, results.length);
		
		}
		
	}


	private int mismatchAt(byte[] last, byte[] results) {
		int i = 0;
		int j = Math.min(last.length, results.length);
		while (i<j) {
			if (last[i]!=results[i]) {
				return i;
			}
			i++;
		}
		if (last.length!=results.length) {
			return i;
		}
		
		return -1;
	}


	private ByteBuffer generateCVSData(int bits) {
		int size = 1<<bits;
		ByteBuffer target = ByteBuffer.allocate(size);
		
		int i = 0;
		byte[] bytes = buildLine(i);		
		while (target.remaining() > bytes.length) {
			target.put(bytes);
			bytes = buildLine(++i);
		}
		target.flip();
		return target;
	}


	private byte[] buildLine(int i) {
		StringBuilder builder = new StringBuilder();
		
		builder.append(i).append(',').append("sometext").append(',').append(Integer.toHexString(i)).append('\n');
		
		return builder.toString().getBytes();
	}


	private ByteVisitor buildLineTestingVisitor() {
		InputStream inputStream = SmallCSVParseTest.class.getResourceAsStream(TEST_FILE);
		if (null == inputStream) {
			try {
				inputStream = new FileInputStream(TEST_FILE);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				fail();
			}
		}
		
		final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				
		
		ByteVisitor testVisitor = new ByteVisitor() {
			boolean debug = false;
			
			
			@Override
			public void visit(byte[] data, int offset, int length) {
				
				try {
					byte[] expected = br.readLine().getBytes();
					assertTrue(new String(expected)+" found:"+new String(Arrays.copyOfRange(data, offset, length+offset)),Arrays.equals(expected, Arrays.copyOfRange(data, offset, length+offset)));
					if (debug) {
						System.err.println("AAA "+new String(data,offset,length));
					}
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				}
				
			}
			
			@Override
			public void visit(byte[] data, int offset1, int length1, int offset2, int length2) {
				byte[] expected;
				try {
					expected = br.readLine().getBytes();
					assertTrue(new String(expected),Arrays.equals(Arrays.copyOfRange(expected, 0, 0+length1), Arrays.copyOfRange(data, offset1, offset1+length1)));
					assertTrue(new String(expected),Arrays.equals(Arrays.copyOfRange(expected, 0+length1, 0+length1+length2), Arrays.copyOfRange(data, offset2, offset2+length2)));
					if (debug) {
						System.err.println("BBB "+new String(data,offset1,length1)+new String(data,offset2,length2));
					}
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				}
				
			}

			@Override
			public void close() {
				
				
			}};
		return testVisitor;
	}
	
	/**
	 * Hardcoded specific test for our specific test file.
	 */
	@Test
	public void testFieldReader() {
		ByteBuffer data = sourceBuffer.asReadOnlyBuffer();				
		Pipe linesRing = new Pipe(linesRingConfig);
		Pipe fieldsRing = new Pipe(fieldsRingConfig);

		linesRing.initBuffers();
		fieldsRing.initBuffers();
		
		GraphManager gm = new GraphManager();
		
		LineSplitterByteBufferStage lineSplitter = new LineSplitterByteBufferStage(gm, data, linesRing);
		
		//this assumes that the ring buffer is large enough to hold the small test file
		lineSplitter.startup();
		lineSplitter.blockingRun();
		lineSplitter.shutdown();
				
		FieldSplitterStage fieldSplitter = new FieldSplitterStage(gm, linesRing, fieldsRing);
		
		//this assumes that the ring buffer is large enough to hold the small test file
		fieldSplitter.startup();
		fieldSplitter.run();
		fieldSplitter.shutdown();
		
	
		
		int countBegins = 0;
		int countEnds = 0;
		int countFlush = 0;
		int countNull = 0;
		StringBuilder accumText = new StringBuilder();
		
		int[] expectedInt = new int[]{0,1970,1,2,934400,-420888,552000,2825600};
		long[] expectedLong = new long[]{10500000000l,-64600000000l};
		int i32 = 0;
		int i64 = 0; 
				
		while (PipeReader.tryReadFragment(fieldsRing)) {
	        	assertTrue(PipeReader.isNewMessage(fieldsRing));
	        	
	        	int msgLoc = PipeReader.getMsgIdx(fieldsRing);
	        	
	        	String name = MetaMessageDefs.FROM.fieldNameScript[msgLoc];
	        	int templateId = (int)MetaMessageDefs.FROM.fieldIdScript[msgLoc];
	        	switch (templateId) {
	        		case 160: //beginMessage
	        			countBegins++;
	        		break;
	        		case 162: //endMessage
	        			countEnds++;
		        	break;
	        		case 62: //flush
	        			countFlush++;
	        	    break;
	        		case 128: //UInt32	        	
	        			int iValue = PipeReader.readInt(fieldsRing, UINT32_VALUE_LOC);
	        			if (fileSpecificTestValues) assertEquals(expectedInt[i32++],iValue);
	        	    break;
	        		case 130: //Int32	        	
	        			int isValue = PipeReader.readInt(fieldsRing, INT32_VALUE_LOC);
	        			if (fileSpecificTestValues) assertEquals(expectedInt[i32++],isValue);
	        	    break;
	        		case 132: //UInt64
	        			long lValue = PipeReader.readLong(fieldsRing, UINT64_VALUE_LOC);
	        			if (fileSpecificTestValues) assertEquals(expectedLong[i64++],lValue);	        			
	        			break;
	        		case 134: //Int64
	        			long lsValue = PipeReader.readLong(fieldsRing, INT64_VALUE_LOC);
	        			if (fileSpecificTestValues) assertEquals(expectedLong[i64++],lsValue);	
	        			break;
	        		case 140: //Decimal
	        			int exp = PipeReader.readDecimalExponent(fieldsRing, DOUBLE_VALUE_LOC);
	        			assertEquals("Test file only uses 2 places of accuracy",2,exp);
	        			
	        			long mant = PipeReader.readDecimalMantissa(fieldsRing, DOUBLE_VALUE_LOC);	        			
	        			float value = PipeReader.readFloat(fieldsRing, DOUBLE_VALUE_LOC);	        			
	        			long computedValue = (long)(Math.rint(value*100f));
	        			
	        			assertEquals("decimal and float versions of this field should be 'near' each other",
	        			 		     mant,computedValue);
	        	    break;
	        		case 136: //ASCII
	        			if (accumText.length()>0) {
	        				accumText.append(',');
	        			}
	        			PipeReader.readASCII(fieldsRing, ASCII_VALUE_LOC, accumText);
	        		
		            break;
	        		case 164: //Null
	        			countNull++;
			        break;
	        	    default:
	        	    	fail("Missing case for:"+templateId+" "+name);
	        	
	        	}
	        	boolean debug = false;
	        	if (debug) {
	        		System.err.println(name+"  "+msgLoc+" "+templateId);
	        	}
	        	PipeReader.releaseReadLock(fieldsRing);
	        	
		 }
		 assertEquals("There must be matching open and close messsage messages",countBegins,countEnds);
		 if (fileSpecificTestValues) assertEquals("Test data file was expected to use null on every row except one",countBegins-1, countNull); //only 1 row in the test data DOES NOT use null
		 assertEquals("End of test data should only send flush once",1,countFlush);
		 if (fileSpecificTestValues) {
			 assertEquals("One of the ASCII fields does not match",
					 	"NYSE:HPQ,Hewlett-packard Co,NYSE:ED,Consolidated Edison Inc,NYSE:AEP,American Electric Power,NYSE:GT,Goodyear Tire & Rubber,NYSE:KO,Coca-cola Co,NYSE:MCD,Mcdonald's Corp",
					 	accumText.toString()); 
		 }
	}
	

	
	/**
	 * This is the example you are looking for
	 * 
	 * TODO: AAA, this test is broken and needs to be fixed ASAP, part of the stage migration. 
	 * 
	 */
	@Ignore
	public void testIngestTemplateThreaded() {
		
		//output target template file
		File temp =null;
		try {
			temp = File.createTempFile("testFile2", "csv");
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		//input test data
		ByteBuffer data = sourceBuffer.asReadOnlyBuffer();				

		//build all the ring buffers
		Pipe linesRing = new Pipe(linesRingConfig);
		Pipe fieldsRing = new Pipe(fieldsRingConfig);
		Pipe flatFileRing = new Pipe(flatFileRingConfig);
		
		GraphManager gm = new GraphManager();
		
		//build all the stages
		LineSplitterByteBufferStage lineSplitter = new LineSplitterByteBufferStage(gm, data, linesRing);
		FieldSplitterStage fieldSplitter = new FieldSplitterStage(gm, linesRing, fieldsRing);
		
				
		MetaMessagesToCSVStage csvBuilderStage = new MetaMessagesToCSVStage(gm, fieldsRing, flatFileRing);
		FileChannel outputFileChannel;
		try {
			outputFileChannel = new RandomAccessFile(temp, "rws").getChannel();
		} catch (FileNotFoundException e1) {
			throw new RuntimeException(e1);
		}   
		FileWriteStage fileWriter = new FileWriteStage(gm, flatFileRing, outputFileChannel);

		StageScheduler ss = new ThreadPerStageScheduler(gm);
		ss.startup();
	
		boolean ok = ss.awaitTermination(10, TimeUnit.SECONDS);
		
		assertEquals("File size does not match:"+temp.getAbsolutePath(),sourceBuffer.remaining(),temp.length());
		
		//load both files
		byte[] expected = new byte[sourceBuffer.remaining()];
		byte[] rebuilt = new byte[sourceBuffer.remaining()];
		
		try {
			InputStream testStream = SmallCSVParseTest.class.getResourceAsStream(TEST_FILE);			
			testStream.read(expected);
			testStream.close();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		try {
			InputStream builtStream = new FileInputStream(temp);
			builtStream.read(rebuilt);
			builtStream.close();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

			
	}


	
	
	
}
