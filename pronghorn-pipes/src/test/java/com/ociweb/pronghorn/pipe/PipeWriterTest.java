package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import org.junit.Test;

public class PipeWriterTest {
	
	@Test
	public void testThis() {
		
		Pipe<RawDataSchema> testPipe = RawDataSchema.instance.newPipe(10, 100);
		testPipe.initBuffers();
		
		long slabPos = Pipe.getSlabHeadPosition(testPipe); //must capture before the message we may want to repeat
		int  blobPos = Pipe.getBlobHeadPosition(testPipe); //must capture before the message we may want to repeat
		
		assertTrue(PipeWriter.tryWriteFragment(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));		
		PipeWriter.writeUTF8(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, "hello world");
		PipeWriter.publishWrites(testPipe);
		
		//now replicate that message again
		assertTrue(PipeWriter.tryReplication(testPipe, slabPos, blobPos));
		PipeWriter.publishWrites(testPipe);
		assertTrue(PipeWriter.tryReplication(testPipe, slabPos, blobPos));
		PipeWriter.publishWrites(testPipe);
		
		long slabPos2 = Pipe.getSlabHeadPosition(testPipe);
		int  blobPos2 = Pipe.getBlobHeadPosition(testPipe);
		
		assertTrue(PipeWriter.tryWriteFragment(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));		
		PipeWriter.writeUTF8(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, "abcdefg");
		PipeWriter.publishWrites(testPipe);
		
		assertTrue(PipeWriter.tryReplication(testPipe, slabPos, blobPos)); //write old one
		PipeWriter.publishWrites(testPipe);
		assertTrue(PipeWriter.tryReplication(testPipe, slabPos2, blobPos2)); //write new one
		PipeWriter.publishWrites(testPipe);
		
		//now read back all
		
		assertTrue(PipeReader.tryReadFragment(testPipe));
		assertEquals("hello world",PipeReader.readUTF8(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, new StringBuilder()).toString());
		PipeReader.releaseReadLock(testPipe);
		
		assertTrue(PipeReader.tryReadFragment(testPipe));
		assertEquals("hello world",PipeReader.readUTF8(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, new StringBuilder()).toString());
		PipeReader.releaseReadLock(testPipe);
		
		assertTrue(PipeReader.tryReadFragment(testPipe));
		assertEquals("hello world",PipeReader.readUTF8(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, new StringBuilder()).toString());
		PipeReader.releaseReadLock(testPipe);
		
		assertTrue(PipeReader.tryReadFragment(testPipe));
		assertEquals("abcdefg",PipeReader.readUTF8(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, new StringBuilder()).toString());
		PipeReader.releaseReadLock(testPipe);
		
		assertTrue(PipeReader.tryReadFragment(testPipe));
		assertEquals("hello world",PipeReader.readUTF8(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, new StringBuilder()).toString());
		PipeReader.releaseReadLock(testPipe);
		
		assertTrue(PipeReader.tryReadFragment(testPipe));
		assertEquals("abcdefg",PipeReader.readUTF8(testPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, new StringBuilder()).toString());
		PipeReader.releaseReadLock(testPipe);
		
	}
	
}
