package com.ociweb.pronghorn.ring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.ring.stream.RingStreams;

public class RingStreamsTest {

	
	@Test
	public void testWriteToOutputStream() {
		
		RingBuffer testRing = new RingBuffer(new RingBufferConfig((byte)4, (byte)13, null,  FieldReferenceOffsetManager.RAW_BYTES));
		
		StringBuilder builder = new StringBuilder();
		
		while (builder.length()<4096) {
			testOneMessage(testRing, builder.toString());
			builder.append((char)('A'+(builder.length()&0x7)));
		}
	}

	public void testOneMessage(RingBuffer testRing, String testString) {
				
		assertEquals(0, RingBuffer.contentRemaining(testRing));		
		
		byte[] testBytes = testString.getBytes();
		
		int blockSize = testRing.maxAvgVarLen;
		RingStreams.writeBytesToRing(testBytes, 0, testBytes.length, testRing, blockSize);
	    RingStreams.writeEOF(testRing);

		ByteArrayOutputStream baost = new ByteArrayOutputStream();
		try {
			RingStreams.writeToOutputStream(testRing, baost);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		String rebuiltMessage = new String(baost.toByteArray());
		assertEquals(testString,rebuiltMessage);
	}
	
	@Test
	public void testReadFromInputStream() {
				
		int testBits = 14;
		int testSize = (1<<testBits)>>2;//data block must not fill full buffer
		
		int lenMask = (1<<(testBits-2))-1;
		
		RingBuffer testRing = new RingBuffer(new RingBufferConfig((byte)6, (byte)17, null,  FieldReferenceOffsetManager.RAW_BYTES));
		
		byte[] testData = new byte[testSize];
		int j = testSize;
		while (--j>=0) {
			testData[j] = (byte)(0xFF&j);
		}
		
		int testIdx = 0;
		int cycleBits = 4;
		int testStop = testSize<<cycleBits;
				
		while (testIdx<testStop) {
			
			final int expectedLength = testIdx&lenMask;
			ByteArrayInputStream inputStream = new ByteArrayInputStream(Arrays.copyOfRange(testData, 0, expectedLength));
		
			try {
				RingStreams.readFromInputStream(inputStream, testRing);		
				RingStreams.writeEOF(testRing);
				
				ByteArrayOutputStream baost = new ByteArrayOutputStream();
				try {
					RingStreams.writeToOutputStream(testRing, baost);
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				}
				
				assertEquals(0, RingBuffer.contentRemaining(testRing));
				
				assertTrue("len:"+expectedLength+" vs "+baost.toByteArray().length, Arrays.equals(Arrays.copyOfRange(testData,0,expectedLength), baost.toByteArray()));
								
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}			
			
			testIdx++;
		}		
		
	}	
	
	
	
	@Test
	public void testRingToRingOutputStream() {
		
		RingBuffer testRing = new RingBuffer(new RingBufferConfig((byte)5, (byte)13, null,  FieldReferenceOffsetManager.RAW_BYTES));
		int blockSize = testRing.maxAvgVarLen;
		
		RingBuffer targetRing = new RingBuffer(new RingBufferConfig((byte)5, (byte)13, null,  FieldReferenceOffsetManager.RAW_BYTES));
		RingOutputStream ringOutputStream = new RingOutputStream(targetRing);
		
		int testBits = 11;
		int testSize = 1<<testBits;
		int testMask = testSize-1;
		
		byte[] testData = new byte[testSize];
		int j = testSize;
		while (--j>=0) {
			testData[j] = (byte)(j&0xFF);
		}
		
		int testIdx = 0;
		int testTotal = testSize*10;
		
		while (testIdx<testTotal) {
			
			int datLen = testIdx & testMask;
			
			assertEquals(0, RingBuffer.contentRemaining(testRing));	
			assertEquals(0, RingBuffer.contentRemaining(targetRing));				

			//Write data into the the ring buffer			
			RingStreams.writeBytesToRing(testData, 0, datLen, testRing, blockSize);
			RingStreams.writeEOF(testRing);
						
			//Here we are reading from one ring and writing to another ring going through an OutputStream
			try {
				RingStreams.writeToOutputStream(testRing, ringOutputStream);
				RingStreams.writeEOF(targetRing);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}
						
			//Now read the data off the target ring to confirm it matches
			ByteArrayOutputStream baost = new ByteArrayOutputStream();
			try {
				RingStreams.writeToOutputStream(targetRing, baost);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}		
				
			assertTrue("len:"+testIdx, Arrays.equals(Arrays.copyOfRange(testData,0,datLen), baost.toByteArray()));
						
			testIdx++;
			
		}		
		
	}

	@Test
	public void testRingToRingOutputStreamByte() {
		
		
		RingBuffer targetRing = new RingBuffer(new RingBufferConfig((byte)10, (byte)15, null,  FieldReferenceOffsetManager.RAW_BYTES));
		
		targetRing.reset((1<<10)-3, 1<<14);			
		
		RingOutputStream ringOutputStream = new RingOutputStream(targetRing);
		
		int testBits = 8;
		int testSize = 1<<testBits;
		int testMask = testSize-1;
		
		byte[] testData = new byte[testSize];
		int j = testSize;
		while (--j>=0) {
			testData[j] = (byte)(j&0xFF);
		}
		
		int testIdx = 0;
		int testTotal = testSize*40;
		
		while (testIdx<testTotal) {
			
			int datLen = testIdx & testMask;
			
			assertEquals(0, RingBuffer.contentRemaining(targetRing));				

			int i = 0;
			while (i < datLen) {
				ringOutputStream.write(testData[i++]);
			}
			ringOutputStream.close();
						
			//Now read the data off the target ring to confirm it matches
			ByteArrayOutputStream baost = new ByteArrayOutputStream();
			try {
				RingStreams.writeToOutputStream(targetRing, baost);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}		
				
			byte[] byteArray = baost.toByteArray();
			assertEquals("test:"+testIdx+" expected len:"+datLen+" data len:"+byteArray.length, datLen, byteArray.length);//Arrays.equals(Arrays.copyOfRange(testData,0,datLen), byteArray));
			
			assertTrue(Arrays.toString(Arrays.copyOfRange(testData,0,datLen))+" vs "+Arrays.toString(byteArray),
					      Arrays.equals(Arrays.copyOfRange(testData,0,datLen),  byteArray));
			 
						
			testIdx++;
			
		}		

	}
	
	
	@Test
	public void testRingToRingInputStream() {
		
		RingBuffer testRing = new RingBuffer(new RingBufferConfig((byte)5, (byte)13, null,  FieldReferenceOffsetManager.RAW_BYTES));
		int blockSize = testRing.maxAvgVarLen;
		RingInputStream ringInputStream = new RingInputStream(testRing);
		
		RingBuffer targetRing = new RingBuffer(new RingBufferConfig((byte)5, (byte)13, null,  FieldReferenceOffsetManager.RAW_BYTES));
		
		int testSize = 3000;
		byte[] testData = new byte[testSize];
		int testIdx = 0;
		
		while (testIdx<testSize) {
			
			assertEquals(0, RingBuffer.contentRemaining(targetRing));	
			

			//Write data into the the ring buffer			
			RingStreams.writeBytesToRing(testData, 0, testIdx, testRing, blockSize);
			
			RingStreams.writeEOF(testRing);
						
			//Here we are reading from one ring and writing to another ring going through an InputStream
			try {
				RingStreams.readFromInputStream(ringInputStream, targetRing);
			
				RingStreams.writeEOF(targetRing);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
	    	}
			
			//Now read the data off the target ring to confirm it matches
			ByteArrayOutputStream baost = new ByteArrayOutputStream();
			try {
				RingStreams.writeToOutputStream(targetRing, baost);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}			
			
			assertTrue("len:"+testIdx, Arrays.equals(Arrays.copyOfRange(testData,0,testIdx), baost.toByteArray()));
			
			testData[testIdx] = (byte)(testIdx&0xFF);
			testIdx++;
			
			
		}		
		
	}
	
	
	@Test
	public void testRingToRingInputStreamBytes() {
		
		RingBuffer testRing = new RingBuffer(new RingBufferConfig((byte)4, (byte)12, null,  FieldReferenceOffsetManager.RAW_BYTES));
		int blockSize = testRing.maxAvgVarLen;
		RingInputStream ringInputStream = new RingInputStream(testRing);
		
		int testSize = 2048;
		byte[] testData = new byte[testSize];
		int testIdx = 0;
		
		while (testIdx<testSize) {
			
			assertEquals(0, RingBuffer.contentRemaining(testRing));				

			int j = 10;
			while (--j>=0) {
			
				//Write data into the the ring buffer			
				RingStreams.writeBytesToRing(testData, 0, testIdx, testRing, blockSize);
				RingStreams.writeEOF(testRing);
											
				ByteArrayOutputStream baost = new ByteArrayOutputStream();
				
				int value;
				try {
					while ( (value=ringInputStream.read())>=0  ) {
						baost.write(value);
					}
					ringInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				}		
		
				
				assertTrue("expectedLen:"+testIdx+" found:"+baost.size(), Arrays.equals(Arrays.copyOfRange(testData,0,testIdx), baost.toByteArray()));
			}
			
			
			testData[testIdx] = (byte)(testIdx&0xFF);
			testIdx++;
			
			
		}		
		
	}
	
	@Test
	public void testRingToRingInputStreamToggleMethods() {
		
		RingBuffer testRing = new RingBuffer(new RingBufferConfig((byte)4, (byte)12, null,  FieldReferenceOffsetManager.RAW_BYTES));
		int blockSize = testRing.maxAvgVarLen;
		RingInputStream ringInputStream = new RingInputStream(testRing);
		
		int testSize = 2048;
		byte[] testData = new byte[testSize];
		int testIdx = 0;
		
		while (testIdx<testSize) {
			
			assertEquals(0, RingBuffer.contentRemaining(testRing));				

			int j = 10;
			while (--j>=0) {
			
				//Write data into the the ring buffer			
				RingStreams.writeBytesToRing(testData, 0, testIdx, testRing, blockSize);
				RingStreams.writeEOF(testRing);
											
				ByteArrayOutputStream baost = new ByteArrayOutputStream();
				
				int value;
				try {
					int buf = 7;
					byte[] tempBuf = new byte[buf];
					
					//This test is toggling between the two primary ways to read from a stream this
					//causes the remaining bytes code inside the input stream to get exercised as it 
					//must span these to calls.
					while ( (value=ringInputStream.read(tempBuf))>=0  ) { //using array read
						baost.write(tempBuf,0,value); 
						
						if ( (value=ringInputStream.read())>=0  ) { //using single byte read
							baost.write(value);
						} else {
							break;
						}
					}
					ringInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				}		
				
				assertTrue("len:"+testIdx+" vs "+baost.toByteArray().length, Arrays.equals(Arrays.copyOfRange(testData,0,testIdx), baost.toByteArray()));
			}
			
			
			testData[testIdx] = (byte)(testIdx&0xFF);
			testIdx++;
			
			
		}		
		
	}
	
}
