package com.ociweb.jfast.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;

public class PrimitivePMAPTest {

	private static final int bufferSize = 4096;
	
	@Test
	public void testWriterSingle() {
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream();
		FASTOutputStream output = new FASTOutputStream(baost);
		
		PrimitiveWriter pw = new PrimitiveWriter(bufferSize, output);
		
		pw.pushPMap(10);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);		
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		
		pw.writePMapBit(1);
		
		pw.popPMap(); //skip should be 7?
		pw.flush();
		
		byte[] data = baost.toByteArray();
		assertEquals("01010101",toBinaryString(data[0]));
		assertEquals("00101010",toBinaryString(data[1]));
		assertEquals("11000000",toBinaryString(data[2]));
		
	}
	
	
	@Test
	public void testWriterNested2() {
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream();
		FASTOutputStream output = new FASTOutputStream(baost);
		
		PrimitiveWriter pw = new PrimitiveWriter(bufferSize, output);
		
		pw.pushPMap(10);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		
		//push will save where we are so we can continue after pop
		pw.pushPMap(3);
			
		    pw.writePMapBit(0);
			pw.writePMapBit(0);
			pw.writePMapBit(0);	
			pw.writePMapBit(1);
			pw.writePMapBit(1);
			pw.writePMapBit(1);
			//implied zero
			
		//continue with parent pmap
		pw.popPMap();
		
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);		
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		
		pw.writePMapBit(1);
		
		pw.popPMap();
		pw.flush();
		
		byte[] data = baost.toByteArray();
		assertEquals("01010101",toBinaryString(data[0]));
		assertEquals("00101010",toBinaryString(data[1]));
		assertEquals("11000000",toBinaryString(data[2]));
		//pmap 2
		assertEquals("10001110",toBinaryString(data[3]));
		
	}
	
	@Test
	public void testWriterNested3() {
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream();
		FASTOutputStream output = new FASTOutputStream(baost);
		
		PrimitiveWriter pw = new PrimitiveWriter(bufferSize, output);
		
		pw.pushPMap(10);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		
		//push will save where we are so we can continue after pop
		pw.pushPMap(3);
			
		    pw.writePMapBit(0);
			pw.writePMapBit(0);
			pw.writePMapBit(0);
			
			pw.pushPMap(4);
				pw.writePMapBit(0);
				pw.writePMapBit(1);
				pw.writePMapBit(0);
				pw.writePMapBit(1);
				pw.writePMapBit(0);
				pw.writePMapBit(1);
				pw.writePMapBit(0);
			pw.popPMap();
			
			pw.writePMapBit(1);
			pw.writePMapBit(1);
			pw.writePMapBit(1);
			//implied zero
			
		//continue with parent pmap
		pw.popPMap();
		
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);		
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		
		pw.writePMapBit(1);
		
		pw.popPMap();
		pw.flush();
		
		byte[] data = baost.toByteArray();
		assertEquals("01010101",toBinaryString(data[0]));
		assertEquals("00101010",toBinaryString(data[1]));
		assertEquals("11000000",toBinaryString(data[2]));
		//pmap 2
		assertEquals("10001110",toBinaryString(data[3]));
		//pmap 3
		assertEquals("10101010",toBinaryString(data[4]));
				
	}
	
	
	@Test
	public void testWriterSequential2() {
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream();
		FASTOutputStream output = new FASTOutputStream(baost);
		
		PrimitiveWriter pw = new PrimitiveWriter(bufferSize, output);
		
		//pw.pushPMap(3);
		
		pw.pushPMap(10);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		pw.writePMapBit(1);
		pw.writePMapBit(0);		
		pw.writePMapBit(1);
		pw.writePMapBit(0);
		
		pw.writePMapBit(1);
		
		pw.popPMap();

		//push will save where we are so we can continue after pop
		pw.pushPMap(3);
			
	    pw.writePMapBit(0);
		pw.writePMapBit(0);
		pw.writePMapBit(0);	
		pw.writePMapBit(1);
		pw.writePMapBit(1);
		pw.writePMapBit(1);
		//implied zero
			
		//continue with parent pmap
		pw.popPMap();
		
		//pw.popPMap();
		
		pw.flush();
		
		byte[] data = baost.toByteArray();
		assertEquals("01010101",toBinaryString(data[0]));
		assertEquals("00101010",toBinaryString(data[1]));
		assertEquals("11000000",toBinaryString(data[2]));
		//pmap 2
		assertEquals("10001110",toBinaryString(data[3]));
		
	}
	
	
	
	//not fast but it gets the job done.
	private String toBinaryString(byte b) {
		String result = Integer.toBinaryString(b);
		if (result.length()>8) {
			return result.substring(result.length()-8,result.length());
		}
		while (result.length()<8) {
			result = "0"+result;
		}
		return result;
	}
	
	@Test
	public void testReaderSingle() {
		
		byte[] testData = new byte[] {((byte)Integer.valueOf("00111010", 2).intValue()),
									    ((byte)Integer.valueOf("10001011", 2).intValue())};
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(testData));
		PrimitiveReader pr = new PrimitiveReader(bufferSize, input);
		
		int maxPMapSize = testData.length; //in bytes
		//open this pmap
		pr.readPMap(maxPMapSize);
		
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		//next byte
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		//unwritten and assumed trailing zeros test
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		//close
		pr.popPMap();
		
	}
	
	@Test
	public void testReaderNested2() {
		
		byte[] testData = new byte[] {((byte)Integer.valueOf("00111010", 2).intValue()),
									    ((byte)Integer.valueOf("10001011", 2).intValue()),
									    //second pmap starts here
									    ((byte)Integer.valueOf("00000000", 2).intValue()),
									    ((byte)Integer.valueOf("11111111", 2).intValue())							    
		};
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(testData));
		PrimitiveReader pr = new PrimitiveReader(bufferSize, input);
		
		//open this pmap
		pr.readPMap(2);
		
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		//stop at this point to load another pmap and read it all before continuing
			pr.readPMap(2);
			//first byte of second pmap
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			//second byte of second pmap
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			//unwritten and assumed trailing zeros test
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			//resume with first pmap
			pr.popPMap();
		///
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		//next byte from first pmap
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		//close
		pr.popPMap();
		
	}
	
	
	@Test
	public void testReaderNested3() {
		
		byte[] testData = new byte[] {((byte)Integer.valueOf("00111010", 2).intValue()),
									    ((byte)Integer.valueOf("10001011", 2).intValue()),
									    //second pmap starts here
									    ((byte)Integer.valueOf("00000000", 2).intValue()),
									    ((byte)Integer.valueOf("11111111", 2).intValue()),
									    //third pmap starts here
									    ((byte)Integer.valueOf("00110011", 2).intValue()),
									    ((byte)Integer.valueOf("11001100", 2).intValue())
									    
		};
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(testData));
		PrimitiveReader pr = new PrimitiveReader(bufferSize, input);
		
		//open this pmap
		pr.readPMap(2);
		
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		//stop at this point to load another pmap and read it all before continuing
			pr.readPMap(2);
			//first byte of second pmap
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			// stop here and load the third pmap
			pr.readPMap(2);
				assertEquals(0,pr.popPMapBit());
				assertEquals(1,pr.popPMapBit());
				assertEquals(1,pr.popPMapBit());
				assertEquals(0,pr.popPMapBit());
				assertEquals(0,pr.popPMapBit());
				assertEquals(1,pr.popPMapBit());
				assertEquals(1,pr.popPMapBit());
				//second byte of third map
				assertEquals(1,pr.popPMapBit());
				assertEquals(0,pr.popPMapBit());
				assertEquals(0,pr.popPMapBit());
				assertEquals(1,pr.popPMapBit());
				assertEquals(1,pr.popPMapBit());
				assertEquals(0,pr.popPMapBit());
				assertEquals(0,pr.popPMapBit());
				
			pr.popPMap();
			//second byte of second pmap
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			//unwritten and assumed trailing zeros test
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			//resume with first pmap
			pr.popPMap();
		///
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		//next byte from first pmap
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		//close
		pr.popPMap();
		
	}
	
    @Test
	public void testDogFood() {
		
    	int pmaps = 3000000;
    	int maxLength = 7;
    	int localBufferSize = pmaps*maxLength;
    	    	
    	byte[][] testPmaps = buildTestPMapData(pmaps, maxLength);
    	
    	//Run through our testing loops without calling any of the methods we want to test
    	//this gives us the total time our testing framework is consuming.
    	//CAUTION: this code must be identical to the code run in the test.
    	long overhead = 0;
    	{
    		int i = pmaps;
    		long start = System.nanoTime();
		    while (--i>=0) {
			    	byte[] pmapData = testPmaps[i];
			    	int j = pmapData.length;
			    	while (--j>=0) {
			    		byte b = pmapData[j];
			    		int ta = (b&1);
			    		int tb = ((b>>1)&1);
			    		int tc = ((b>>2)&1);
			    		int td = ((b>>3)&1);
			    		int te = ((b>>4)&1);
			    		int tf = ((b>>5)&1);
			    		int tg = ((b>>6)&1);
			    		int th = ((b>>7)&1);
			    	}			   
		    }
		    overhead = (System.nanoTime()-start);
    	}
    	
    	////////////////////////////////////
    	//compute max bytes written to stream based on test data
    	////////////////////////////////////
    	int maxWrittenBytes = (int)Math.ceil((maxLength*8)/7d);
    	    	
    	////////////////////////////////
    	//write all the bytes to the writtenBytes array
    	//this is timed to check performance
    	//same array will be verified for correctness in the next step.
    	//////////////////////////////
    	
		ByteArrayOutputStream baost = new ByteArrayOutputStream(localBufferSize);
		FASTOutputStream output = new FASTOutputStream(baost);
		
		PrimitiveWriter pw = new PrimitiveWriter(localBufferSize, output);
		
		byte[] writtenBytes;
		
		int i = pmaps;
	    try {
	    	long start = System.nanoTime();
		    while (--i>=0) {
			    	byte[] pmapData = testPmaps[i];
			    	//none of these are nested, we don't want to test nesting here.
			    	pw.pushPMap(maxWrittenBytes); //many are shorter but we want to test the trailing functionality
			    	int j = pmapData.length;
			    	//j=0 1 byte written - no bits are sent so its all 7 zeros
			    	//j=1 2 bytes written  (bytes * 8)/7 to compute bytes written
			    	//j=2 3 bytes written
			    	//j=3 4 bytes written
			    				    	
			    	while (--j>=0) {
			    		
			    		byte b = pmapData[j];
			    					    		
			    		//put in first byte
			    		pw.writePMapBit(b&1);
			    		pw.writePMapBit((b>>1)&1);
			    		pw.writePMapBit((b>>2)&1);
			    		pw.writePMapBit((b>>3)&1);
			    		pw.writePMapBit((b>>4)&1);
			    		pw.writePMapBit((b>>5)&1);
			    		pw.writePMapBit((b>>6)&1);
			    		//put in next byte
			    		pw.writePMapBit((b>>7)&1);
			    		//6 zeros are assumed 
			    		
			    	}			    	
			    	
			    	pw.popPMap();
	
		    }
		    long duration = (System.nanoTime()-start);
		    writtenBytes = baost.toByteArray();
		    
		    if (duration>overhead) {
		    	duration -= overhead;
		    } else {
		    	System.err.println();
		    	System.err.println("unable to compute overhead measurement. per byte:"+(overhead/(double)writtenBytes.length));
		    	
		    	overhead = 0;
		    }
		    assertTrue("total duration: "+duration+"ns but nothing written.",writtenBytes.length>0);
		    
		    double nsPerByte = duration/(double)writtenBytes.length; 
		    System.err.println("PMap write "+nsPerByte+"ns per byte");
	    } finally {
	    	int testedMaps = (pmaps-(i+1));
	    	System.err.println("finished testing write after "+testedMaps+" unique pmaps and testing overhead of "+overhead);
	    }
	    
	    ////////////////////////////////////
	    //read all the bytes from the written bytes array to ensure
	    //that they all match with the original test data
	    //this has extra unit test logic in it so it is NOT timed for performance
	    ///////////////////////////////////
	    
		i = pmaps;
    	try {
    		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(writtenBytes));
    		PrimitiveReader pr = new PrimitiveReader(bufferSize, input);
		    while (--i>=0) {
			    	byte[] pmapData = testPmaps[i];
			    	pr.readPMap(maxWrittenBytes);
			    	
			    	int j = pmapData.length;
			    	if (j==0) {
			    		assertEquals(0,pr.popPMapBit());
			    		assertEquals(0,pr.popPMapBit());
			    		assertEquals(0,pr.popPMapBit());
			    		assertEquals(0,pr.popPMapBit());
			    		assertEquals(0,pr.popPMapBit());
			    		assertEquals(0,pr.popPMapBit());
			    		//assertEquals(0,pr.popPMapBit()); //TODO: this should have been zero? but the early write is causing second byte?
			    		
			    	} else {
			    		int totalBits = maxWrittenBytes*7;
				    	while (--j>=0) {
				    		byte b = pmapData[j];
				    						    		
				    		assertEquals((b&1),pr.popPMapBit());
				    		assertEquals(((b>>1)&1),pr.popPMapBit());
				    		assertEquals(((b>>2)&1),pr.popPMapBit());
				    		assertEquals(((b>>3)&1),pr.popPMapBit());
				    		assertEquals(((b>>4)&1),pr.popPMapBit());
				    		assertEquals(((b>>5)&1),pr.popPMapBit());
				    		assertEquals(((b>>6)&1),pr.popPMapBit());
				    		assertEquals(((b>>7)&1),pr.popPMapBit());
				    		
				    		totalBits-=8;
				    	}	
				    	//confirm the rest of the "unwriten bits are zeros"
				    	while (--totalBits>=0) {
				    		assertEquals(0,pr.popPMapBit());
				    	}				    	
			    	}
			    	pr.popPMap();
		    }
		    
    	} finally {
    		int readTestMaps = (pmaps-(i+1));
    		System.err.println("finished testing read after "+readTestMaps+" unique pmaps and testing overhead of "+overhead);
    	}
    	
    	/////////////////////////////////////////
    	//test read pmap timing
    	////////////////////////////////////////
		i = pmaps;
    	try {
    		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(writtenBytes));
    		PrimitiveReader pr = new PrimitiveReader(bufferSize, input);
    		long start = System.nanoTime();
		    while (--i>=0) {
			    	byte[] pmapData = testPmaps[i];
			    	pr.readPMap(maxWrittenBytes);
			    	
			    	int j = pmapData.length;
			    	while (--j>=0) {
			    		pr.popPMapBit();
			    		pr.popPMapBit();
			    		pr.popPMapBit();
			    		pr.popPMapBit();
			    		pr.popPMapBit();
			    		pr.popPMapBit();
			    		pr.popPMapBit();
			    		pr.popPMapBit();
			    	}	
			    	
			    	pr.popPMap();
		    }
		    long duration = (System.nanoTime()-start) - overhead;
		    		    
		    double nsPerByte = duration/(double)writtenBytes.length; 
		    System.err.println("PMap read "+nsPerByte+"ns per byte");
    	} finally {
    		int readTestMaps = (pmaps-(i+1));
    		System.err.println("finished testing read after "+readTestMaps+" unique pmaps and testing overhead of "+overhead);
    	}
		//  */
	}


	private byte[][] buildTestPMapData(int pmaps, int maxLength) {
		byte[][] testPmaps = new byte[pmaps][];
    	
    	double r = 0d;
    	double step = (2d*Math.PI)/(double)(pmaps);
    	
    	int i = pmaps;
    	int len = maxLength;
    	while (--i>=0) {
    	    byte[] newPmap = new byte[len];
    	    int j = len;
    	    while (--j>=0) {
    	    	//this produces large repeatable test data with a good coverage
    	    	newPmap[j] = (byte)(0xFF&(int)(128*Math.sin(r)));
    	    	r+=step;
    	    }
    	  //  System.err.println(Arrays.toString(newPmap));
    		testPmaps[i] = newPmap;
    		if (--len<0) {
    			len = maxLength;
    		}
    	}
		return testPmaps;
	}
	
	
}
