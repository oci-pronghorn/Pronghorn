package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.junit.Ignore;
import org.junit.Test;

public class PipeTest {

	@Test
	public void addLongtest() {
		// put on pipe, take them off confirm same.
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers();

		// confirm pipe empty
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_LONG_50);

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_LONG_50), msgsize);
		Pipe.addLongValue(1234L, p);

		// confirm that we have written record
		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_LONG_50);

		long val1 = Pipe.takeLong(p);

		assertEquals(val1, 1234L);

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

	@Test
	public void addInttest() {
		// public static final int MSG_INT_40 = 0x0000000b;
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers(); // init pipe

		// expected always first
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_INT_40);

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_INT_40), msgsize);
		Pipe.addIntValue(1, p);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_INT_40);
		int val1 = Pipe.takeInt(p);

		assertEquals(1, val1);

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);
	}

	@Test
	public void addByteArraytest() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers(); // init pipe

		// expected always first
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);
		byte[] arr = { 1, 2, 3 };
		Pipe.addByteArray(arr, p);// adding the field(int)

		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		byte[] arr1 = new byte[3];
		Pipe.readBytes(p, arr1, 0, arr1.length - 1, 0, arr1.length);
		for (int i = 0; i < arr1.length; i++) {
			assertEquals(arr[i], arr1[i]);
		}

		arr1 = new byte[3];
		ByteBuffer buff = ByteBuffer.wrap(arr1);
		Pipe.readBytes(p, buff, 0, 3);
		for (int i = 0; i < arr1.length; i++) {
			assertEquals(arr[i], arr1[i]);
		}

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);
	}

	@Test
	public void addUTF8test() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(5, 100);
		p.initBuffers(); // init pipe

		// expected always first
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);

		CharSequence x = "#$ԅԔxxx";

		Pipe.addUTF8(x, p);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		byte[] arr = { 1, 2, 3 };

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		StringBuilder str = new StringBuilder();
		int meta = Pipe.takeRingByteMetaData(p);
		int length = Pipe.takeRingByteLen(p);
		Pipe.readUTF8(p, str, meta, length);

		assertEquals("#$ԅԔxxx", str.toString());

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

	@Test
	public void addASCIItest() {

		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(5, 100);
		p.initBuffers(); // init pipe

		// expected always first
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);

		CharSequence source = "1234abcd";

		Pipe.addASCII(source, p);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		byte[] arr = { 1, 2, 3 };

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		StringBuilder str = new StringBuilder();
		Pipe.readASCII(p, str, 0, source.length());

		assertEquals("1234abcd", str.toString());
		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

	@Test
	public void addIntAsASCIItest() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers(); // init pipe

		// confirm pipe empty
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);
		Pipe.addIntAsASCII(p, 123);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		// long val1 = Pipe.takeLong(p); //actual value written above
		StringBuilder str = new StringBuilder();
		Pipe.addIntAsASCII(p, 456); // to see if multiple work.
		Pipe.readASCII(p, str, 0, 6);
		assertEquals("123456", str.toString());

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

	@Test
	public void addLongasASCIItest() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(10, 200);
		p.initBuffers();

		int val = Pipe.bytesOfContent(p);

		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);

		Pipe.addLongAsASCII(p, 1234L);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		StringBuilder str = new StringBuilder();

		int meta = Pipe.takeRingByteMetaData(p);
		int length = Math.max(0, Pipe.takeRingByteLen(p));
		int position = Pipe.bytePosition(meta, p, length);

		Pipe.readASCII(p, str, meta, length);

		assertEquals("1234", str.toString());

		Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);

		str = new StringBuilder();
		Pipe.addLongAsUTF8(p, 9909L);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		meta = Pipe.takeRingByteMetaData(p);
		length = Math.max(0, Pipe.takeRingByteLen(p));
		position = Pipe.bytePosition(meta, p, length);

		Pipe.readUTF8(p, str, 4, 4);
		assertEquals("9909", str.toString());
		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

	@Test
	public void writeToOutputStreamtest() throws IOException {

		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers();

		// expected always first
		int val = Pipe.bytesOfContent(p);

		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);
		byte[] arr = { 1, 2, 3 };
		Pipe.addByteArray(arr, p);

		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		ByteArrayOutputStream outputstr = new ByteArrayOutputStream();

		Pipe.writeFieldToOutputStream(p, outputstr);

		assertEquals(outputstr.size(), 3);
		outputstr.write(1);
		byte[] b = outputstr.toByteArray();
		for (int i = 0; i < arr.length; i++)
			assertEquals(arr[i], b[i]);

	}

	@Test
	public void copyASCIItoBytesTest() {

		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers(); // init pipe

		int val = Pipe.bytesOfContent(p);

		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); // byte
																				// array??

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);

		char[] source = { 'h', 'e', 'l', 'l', 'o' };

		int result = Pipe.copyASCIIToBytes(source, 0, source.length, p);

		byte[] arr1 = new byte[5];

		Pipe.readBytes(p, arr1, 0, arr1.length - 1, 0, arr1.length);

		for (int i = 0; i < arr1.length; i++) {
			assertEquals(arr1[i], (byte) source[i]);
		}

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

	@Test
	public void copyUTF8ToBytetest() {

		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers();

		int val = Pipe.bytesOfContent(p);

		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);

		char[] source = { 'µ', '¢', '£', '€', '«', '»' };
		Pipe.copyUTF8ToByte(source, 0, source.length, p);

		byte[] arr1 = new byte[15];
		Pipe.readBytes(p, arr1, 0, arr1.length, 0, arr1.length);

		byte arr2[] = new byte[15];

		String value = new String(arr1);
		String expected = new String(source);

		assertEquals(value.trim(), expected.trim());
	}

	@Test
	public void addByteBuffertest() {

		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers(); // init pipe

		int val = Pipe.bytesOfContent(p);

		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);

		byte[] source = { 1, 2, 3, 4, 5 };
		ByteBuffer buffer = ByteBuffer.wrap(source);

		Pipe.addByteBuffer(buffer, p);

		byte[] arr1 = new byte[5];
		Pipe.readBytes(p, arr1, 0, arr1.length, 0, arr1.length);

		// second add buffer test
		buffer = ByteBuffer.wrap(source);
		Pipe.addByteBuffer(buffer, source.length, p);

		byte[] arr2 = new byte[5];
		Pipe.readBytes(p, arr2, 0, arr2.length, 0, arr2.length);

		for (int i = 0; i < source.length; i++) {
			assertEquals(arr1[i], source[i]);
			assertEquals(arr2[i], source[i]);
		}

	}

	@Ignore
	public void resetTest() {

		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		boolean isinit = Pipe.isInit(p);

		p.initBuffers();

		int val = Pipe.bytesOfContent(p);
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_INT_40);

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_INT_40), msgsize);
		Pipe.addIntValue(1, p);

		Pipe.confirmLowLevelWrite(p, msgsize);
		Pipe.publishWrites(p);

		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_INT_40);

		assertEquals(3, Pipe.workingHeadPosition(p));
		p.reset(); // test of reset.
		assertEquals(0, Pipe.workingHeadPosition(p));

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

	@Ignore
	public void copyFragmentTest() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		Pipe<TestDataSchema> p1 = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers(); // init pipe
		p1.initBuffers();

		int msgsize = Pipe.addMsgIdx(p1, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		int msgsize2 = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); // byte

		byte[] arr = { 1, 2, 3 };
		Pipe.addByteArray(arr, p);
		Pipe.addByteArray(arr, p1);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		Pipe.copyFragment(p, p1);
		byte[] arr1 = new byte[6];

		Pipe.readBytes(p1, arr1, 0, arr1.length, 0, arr1.length); //
		byte[] expected = { 1, 2, 3, 1, 2, 3 };
		for (int i = 0; i < arr1.length; i++) {
			assertEquals(expected[i], arr1[i]);
		}

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);
		Pipe.confirmLowLevelRead(p1, msgsize);
		Pipe.releaseReadLock(p1);

	}

	@Ignore
	public void validatePipeBlobHasDataToRead() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers();
		int val = Pipe.bytesOfContent(p);

		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_INT_40);

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_INT_40), msgsize);
		Pipe.addIntValue(1, p);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		boolean bool = Pipe.validatePipeBlobHasDataToRead(p, 0, 1);

		assertTrue(!bool); // negative test case. only write to in case of
							// string

		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_INT_40);

		int val1 = Pipe.takeInt(p);

		assertEquals(1, val1);

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

		Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);
		Pipe.addASCII("test", p);

		int meta = Pipe.takeRingByteMetaData(p); // just assume this is correct.
		int length = Math.max(0, Pipe.takeRingByteLen(p));
		int position = Pipe.bytePosition(meta, p, length);

		bool = Pipe.validatePipeBlobHasDataToRead(p, position, length);
		assertTrue(bool);

		Pipe.releaseReadLock(p);

	}

	@Ignore
	public void hasRoomForWritetest() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
		p.initBuffers();
		int val = Pipe.bytesOfContent(p);

		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_INT_40);

		boolean hasRoom = Pipe.hasRoomForWrite(p);

		assertTrue(hasRoom);
		hasRoom = Pipe.hasRoomForWrite(p, 1);

		assertTrue(hasRoom);

		Pipe.addIntValue(3, p);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		boolean canRead = Pipe.hasContentToRead(p);
		assertTrue(canRead);

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

}