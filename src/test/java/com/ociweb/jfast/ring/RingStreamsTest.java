package com.ociweb.jfast.ring;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;

public class RingStreamsTest {

	
	@Test
	public void testWriteToOutputStream() {
		
		RingBuffer testRing = new RingBuffer((byte)4,(byte)12);
		
		StringBuilder builder = new StringBuilder();
		
		while (builder.length()<4096) {
			testOneMessage(testRing, builder.toString());
			builder.append((char)('A'+(builder.length()&0x7)));
		}
	}

	public void testOneMessage(RingBuffer testRing, String testString) {
				
		assertEquals(0, RingBuffer.contentRemaining(testRing));		
		
		byte[] testBytes = testString.getBytes();
		
		int blockSize = testRing.byteMask/(testRing.mask>>1);
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
				
		RingBuffer testRing = new RingBuffer((byte)4,(byte)12);
		
		StringBuilder builder = new StringBuilder();
		
		while (builder.length()<2048) {
			
			String testString = builder.toString();
			ByteArrayInputStream inputStream = new ByteArrayInputStream(testString.getBytes());
		
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
				
				String rebuiltMessage = new String(baost.toByteArray());
				assertEquals(testString,rebuiltMessage);
				
				
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}			
			
			builder.append((char)('A'+(builder.length()&0x7)));
		}		
		
	}	
	
	@Test
	public void testRingToRingOutputStream() {
		
		RingBuffer testRing = new RingBuffer((byte)4,(byte)12);
		int blockSize = testRing.byteMask/(testRing.mask>>1);
		
		RingBuffer targetRing = new RingBuffer((byte)4, (byte)12);
		RingOutputStream ringOutputStream = new RingOutputStream(targetRing);
		
		StringBuilder builder = new StringBuilder();
		
		while (builder.length()<3000) {
			String testString = builder.toString();
			assertEquals(0, RingBuffer.contentRemaining(testRing));	
			assertEquals(0, RingBuffer.contentRemaining(targetRing));	
			

			//Write data into the the ring buffer
			byte[] testBytes = testString.getBytes();			
			RingStreams.writeBytesToRing(testBytes, 0, testBytes.length, testRing, blockSize);
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
			
			String rebuiltMessage = new String(baost.toByteArray());
			assertEquals(testString,rebuiltMessage);
			builder.append((char)('A'+(builder.length()&0x7)));
						
		}		
		
	}
	
	
	@Test
	public void testRingToRingInputStream() {
		
		RingBuffer testRing = new RingBuffer((byte)4,(byte)12);
		int blockSize = testRing.byteMask/(testRing.mask>>1);
		RingInputStream ringInputStream = new RingInputStream(testRing);
		
		RingBuffer targetRing = new RingBuffer((byte)4, (byte)12);
		
		StringBuilder builder = new StringBuilder();
		
		while (builder.length()<3000) {
			String testString = builder.toString();
			assertEquals(0, RingBuffer.contentRemaining(testRing));	
			assertEquals(0, RingBuffer.contentRemaining(targetRing));	
			

			//Write data into the the ring buffer
			byte[] testBytes = testString.getBytes();			
			RingStreams.writeBytesToRing(testBytes, 0, testBytes.length, testRing, blockSize);
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
			
			String rebuiltMessage = new String(baost.toByteArray());
			assertEquals(testString,rebuiltMessage);
			builder.append((char)('A'+(builder.length()&0x7)));
						
		}		
		
	}
	
	
}
