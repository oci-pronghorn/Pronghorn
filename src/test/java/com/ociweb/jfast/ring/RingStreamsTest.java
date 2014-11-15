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
		RingStreams.writeBytesToRing(testBytes, testRing, blockSize);
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
	
}
