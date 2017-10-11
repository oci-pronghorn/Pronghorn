package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.junit.Test;

//****************************should go back and change tests so expected val is first. and also word 'test' is at end.
public class PipeTest {

	// methods with optional fields, dont test.

	// test long, ints, utf8, byte array ascii arrays.

	// test take add int.
	// take add arrays.
	// write utf8.
	// write ascii (american chars)

	// schema
	// byte array(rand nums)

	// ascii/utf8 are setn as byte arrays.
	// for ex. send MSG_CHUNKEDSTREAM_10 to tell pipe that byte array is
	// incoming. then send byte array.

	@Test
	public void addLongtest() {
		// put on pipe, take them off confirm same.
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); // 4
																			// is
																			// the
																			// floor(?)
																			// messages
																			// 100-
																			// max
																			// length
																			// for
																			// any
																			// message.
																			// max
																			// length
																			// of
																			// 100
																			// byte
																			// sting,
																			// 100
																			// byte
																			// array
																			// etc
		p.initBuffers(); // init pipe

		// confirm pipe empty
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_LONG_50);
		// have to write fields in order thtey appeared, cant skip any, have to
		// write all
		// record identifier comes first followed by field(s)
		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_LONG_50), msgsize);
		Pipe.addLongValue(1234L, p);// adding the field ( look at MSG)

		// confirm that we have writtenr record
		Pipe.confirmLowLevelWrite(p, msgsize); // magic number represents
												// metadata

		Pipe.publishWrites(p); // confirm publish go together. now consumer can
								// read data

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_LONG_50); // same as line 22.
															// mesg started
		long val1 = Pipe.takeLong(p); // actual value written above

		assertEquals(val1, 1234L); // assert long written is what we recieve

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p); // ring buffer- circular array... no out of
									// bounds.
		// releasing record so can 'come back around' and write on top of it

	}

	@Test
	public void addInttest() {
		// public static final int MSG_INT_40 = 0x0000000b;
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); // 4
																			// is
																			// the
																			// floor(?)
																			// messages
																			// 100-
																			// max
																			// length
																			// for
																			// any
																			// message.
																			// max
																			// length
																			// of
																			// 100
																			// byte
																			// sting,
																			// 100
																			// byte
																			// array
																			// etc
		p.initBuffers(); // init pipe

		// expected always first
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_INT_40);

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_INT_40), msgsize);
		Pipe.addIntValue(1, p);
		;// adding the field(int)

		Pipe.confirmLowLevelWrite(p, msgsize); // magic number represents
												// metadata

		Pipe.publishWrites(p); // confirm publish go together. now consumer can
								// read data

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_INT_40); // same as line 22.
															// mesg started
		int val1 = Pipe.takeInt(p); // actual value written above

		
		
		assertEquals(1, val1); // assert long written is what we recieve

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p); // ring buffer- circular array... no out of
									// bounds.
		// releasing record so can 'come back around' and write on top of it

	}

	@Test
	public void addByteArraytest() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); 
		p.initBuffers(); // init pipe

		// expected always first
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); // byte
																				// array??

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);
		byte[] arr = { 1, 2, 3 };
		Pipe.addByteArray(arr, p);// adding the field(int)

		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10); // same as
																	// line 22.
																	// mesg
																	// started

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		byte[] arr1 = new byte[3];// backing array for byte buffer below. will
									// capture byte array written

		Pipe.readBytes(p, arr1, 0, arr1.length - 1, 0, arr1.length);
		for (int i = 0; i < arr1.length; i++) {
			assertEquals(arr[i], arr1[i]);
		}

		// now we will test byte buffer method for reading bytes.
		arr1 = new byte[3];
		ByteBuffer buff = ByteBuffer.wrap(arr1);
		Pipe.readBytes(p, buff, 0, 3);
		for (int i = 0; i < arr1.length; i++) {
			assertEquals(arr[i], arr1[i]);
		}

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p); // ring buffer- circular array... no out of
									// bounds.
		// releasing record so can 'come back around' and write on top of it

	}

	@Test
	public void addUTF8test() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(5, 100);
		p.initBuffers(); // init pipe

		// expected always first
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); // utf8
																				// is
																				// chunkedstream?

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);

		// skips character after weird Ascii value.******************. each time
		// there is a strange char it shrinks the lenght by 1...
		CharSequence x = "#$ԅԔxxx";
		// *******************************************************************
		Pipe.addUTF8(x, p);

		// source is byte array here. Pipe.addUTF8(source, sourceCharCount, rb);
		Pipe.confirmLowLevelWrite(p, msgsize); // magic number represents
												// metadata

		Pipe.publishWrites(p); // confirm publish go together. now consumer can
								// read data

		byte[] arr = { 1, 2, 3 };

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10); // same as
																	// line 22.
																	// mesg
																	// started
		StringBuilder str = new StringBuilder();
		int meta = Pipe.takeRingByteMetaData(p);
		int length =Pipe.takeRingByteLen(p);
		Pipe.readUTF8(p, str, meta, length); // reads utf8 charsequence above
												// into strinbguilder
		//System.out.println(str.length());
		//System.out.println(str.toString());

		assertEquals("#$ԅԔxxx", str.toString()); // assert long written is what
													// we recieve

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

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); // utf8
																				// is
																				// chunkedstream?

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);

		// skips character after weird Ascii value.******************. each time
		// there is a strange char it shrinks the lenght by 1...
		CharSequence source = "1234abcd";
		// *******************************************************************
		Pipe.addASCII(source, p);

		// source is byte array here. Pipe.addUTF8(source, sourceCharCount, rb);
		Pipe.confirmLowLevelWrite(p, msgsize); // magic number represents
												// metadata

		Pipe.publishWrites(p); // confirm publish go together. now consumer can
								// read data

		byte[] arr = { 1, 2, 3 };

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10); // same as
																	// line 22.
																	// mesg
																	// started
		StringBuilder str = new StringBuilder();
		Pipe.readASCII(p, str, 0, source.length());
	//	System.out.println(str.length());
	//	System.out.println(str.toString());

		assertEquals("1234abcd", str.toString()); // assert long written is what
													// we recieve

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

	@Test
	public void addIntAsASCIItest() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); // 4
																			// is
																			// the
																			// floor(?)
																			// messages
																			// 100-
																			// max
																			// length
																			// for
																			// any
																			// message.
																			// max
																			// length
																			// of
																			// 100
																			// byte
																			// sting,
																			// 100
																			// byte
																			// array
																			// etc
		p.initBuffers(); // init pipe

		// confirm pipe empty
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); // No
																				// idea
																				// if
																				// this
																				// is
																				// correct...*******

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);
		Pipe.addIntAsASCII(p, 123);

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10); // same as
																	// line 22.
																	// mesg
																	// started
		// long val1 = Pipe.takeLong(p); //actual value written above
		StringBuilder str = new StringBuilder();
		Pipe.addIntAsASCII(p, 456); // to see if multiple work.
		Pipe.readASCII(p, str, 0, 6);
		assertEquals("123456", str.toString());

		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);

	}

	// i have been doing chucked stream for int/long to ascii, seems to be
	// working fine.
	@Test
	public void addLongasASCIItest() {
		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(10, 200); 
		p.initBuffers(); // init pipe

		// confirm pipe empty
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); 

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);

		Pipe.addLongAsASCII(p, 1234L);	
		
		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

		// now read from pipe************
		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10); // same as
																	// line 22.
													// mesg
																	// started
		// long val1 = Pipe.takeLong(p); //actual value written above
		StringBuilder str = new StringBuilder();
		
		 int meta = Pipe.takeRingByteMetaData(p); //just assume this is correct.
			int length    = Math.max(0, Pipe.takeRingByteLen(p));
			  int position = Pipe.bytePosition(meta, p, length);
		
		Pipe.readASCII(p, str, meta, length);

	
		
		
		assertEquals("1234",str.toString());
		
		Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); 
		
		
		
		str = new StringBuilder();
		Pipe.addLongAsUTF8(p, 9909L);
	
		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);
		
		  meta = Pipe.takeRingByteMetaData(p); //just assume this is correct.
			 length    = Math.max(0, Pipe.takeRingByteLen(p));
			   position = Pipe.bytePosition(meta, p, length);
		
		Pipe.readUTF8(p, str, 4, 4);
		assertEquals("9909",str.toString());
		Pipe.confirmLowLevelRead(p, msgsize);
		Pipe.releaseReadLock(p);
		

	

	}
	
	
//	@Test
//	public void addLongasUTF8test() {
//		
//	}

	// writeFieldToOutputStream and read/build/copy from input stream.
	@Test
	public void writeToOutputStreamtest() throws IOException {

		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); 
		p.initBuffers(); // init pipe

		// expected always first
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); // byte
																				// array??

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);
		byte[] arr = { 1, 2, 3 };
		Pipe.addByteArray(arr, p);// adding the field(int)

		int msgidx = Pipe.takeMsgIdx(p);
		assertEquals(msgidx, TestDataSchema.MSG_CHUNKEDSTREAM_10); // same as
																	// line 22.
																	// mesg
																	// started

		Pipe.confirmLowLevelWrite(p, msgsize);

		Pipe.publishWrites(p);

	
	

	
		ByteArrayOutputStream outputstr = new ByteArrayOutputStream();

		// ****
		// it knows the size of the pipe or it wouldnt write 3 zeros. am i
		// forgetting to do something?

		Pipe.writeFieldToOutputStream(p, outputstr);
	//	System.out.println(outputstr.size());
		assertEquals(outputstr.size(),3);
		outputstr.write(1);
		byte[] b = outputstr.toByteArray(); // ?? not working either. should
											// have an array of 123
		for (int i = 0;i<arr.length;i++)
			assertEquals(arr[i], b[i]);
	
		

	}
@Test
	public void copyASCIItoBytesTest() {
		// arent going to send anything through pipe, just use this copy
		// function

		Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); 
		p.initBuffers(); // init pipe

		// expected always first
		int val = Pipe.bytesOfContent(p);

		// expected always first
		assertEquals(0, val);

		int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); // byte
																				// array??

		assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);
		
										

		char[] source = { 'h', 'e', 'l', 'l', 'o' };

		int result = Pipe.copyASCIIToBytes(source, 0, source.length, p);

byte[] arr1 = new byte[5];

Pipe.readBytes(p, arr1, 0, arr1.length - 1, 0, arr1.length);

//testing char array has copied over as byte array
for(int i =0;i<arr1.length;i++){
	assertEquals(arr1[i],(byte)source[i]);
}



Pipe.confirmLowLevelRead(p, msgsize);
Pipe.releaseReadLock(p); 


		
	}
@Test
public void copyUTF8ToBytetest(){
	
	
	
	Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); 
	p.initBuffers(); // init pipe

	// expected always first
	int val = Pipe.bytesOfContent(p);

	// expected always first
	assertEquals(0, val);

	int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); 

	assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);
	
	
	char [] source = { 'µ', '¢' ,'£' ,'€' ,'«', '»'}; //?? look at ouput. why is it returning diff utf8 characters?
//char [] source = {'1','2','3','4','5'}; // works fine with ascii.
	//System.out.println("source:");
	for (char c : source){
		//System.out.println(c);
	}
	//System.out.println("\n");
	Pipe.copyUTF8ToByte(source,  0, source.length, p);
	

byte[] arr1 = new byte[15];
Pipe.readBytes(p, arr1, 0, arr1.length, 0, arr1.length);

byte arr2[] = new byte[15]; // at most 6 bytes per utf8 character

String value = new String(arr1);
String expected  = new String(source);



//issue is utf8 chars going in are not the same as chars coming out. encoding error on my side?
	assertEquals(value.trim(),expected.trim());
}

@Test 
public void addByteBuffertest(){

	Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); 
	p.initBuffers(); // init pipe

	// expected always first
	int val = Pipe.bytesOfContent(p);

	// expected always first
	assertEquals(0, val);

	int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); 

	assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_CHUNKEDSTREAM_10), msgsize);
	
	byte [] source = {1,2,3,4,5};
	ByteBuffer buffer = ByteBuffer.wrap(source);
	
	Pipe.addByteBuffer(buffer, p);
	

byte[] arr1 = new byte[5];
Pipe.readBytes(p, arr1, 0, arr1.length, 0, arr1.length);

//second add buffer test
buffer =  ByteBuffer.wrap(source);
Pipe.addByteBuffer(buffer,source.length, p);

byte[] arr2 = new byte[5];
Pipe.readBytes(p, arr2, 0, arr2.length, 0, arr2.length);

//testing char array has copied over as byte array
for(int i =0;i<source.length;i++){
	assertEquals(arr1[i],source[i]);
	assertEquals(arr2[i],source[i]);
}
	
}




@Test
public void resetTest(){

	Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
	boolean isinit = Pipe.isInit(p);

	p.initBuffers(); 
	

	int val = Pipe.bytesOfContent(p);
	assertEquals(0, val);

	int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_INT_40);

	assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_INT_40), msgsize);
	Pipe.addIntValue(1, p);
	

	Pipe.confirmLowLevelWrite(p, msgsize); //
	Pipe.publishWrites(p); 

	// now read from pipe************
	int msgidx = Pipe.takeMsgIdx(p);
	assertEquals(msgidx, TestDataSchema.MSG_INT_40); 
	
	assertEquals(3,Pipe.workingHeadPosition(p));
	p.reset(); // test of reset.
	assertEquals(0,Pipe.workingHeadPosition(p));
	
	Pipe.confirmLowLevelRead(p, msgsize);
	Pipe.releaseReadLock(p);
	
}

@Test
public void copyFragmentTest(){
	Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100);
	Pipe<TestDataSchema> p1 = TestDataSchema.instance.newPipe(4, 100);
p.initBuffers(); // init pipe
p1.initBuffers();

int msgsize = Pipe.addMsgIdx(p1, TestDataSchema.MSG_CHUNKEDSTREAM_10);
int msgsize2 = Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10); // byte

byte[] arr = { 1, 2, 3 };
Pipe.addByteArray(arr, p);// adding the field(int)
Pipe.addByteArray(arr, p1);


Pipe.confirmLowLevelWrite(p, msgsize);

Pipe.publishWrites(p);


Pipe.copyFragment(p,p1); // this will copy p to p1. p will be 1,2,3, p1 will now be 1,2,3,1,2,3.

byte[] arr1 = new byte[6];// backing array for byte buffer below. will
// capture byte array written

Pipe.readBytes(p1, arr1, 0, arr1.length, 0, arr1.length); //
byte [] expected = {1,2,3,1,2,3};
for (int i = 0; i < arr1.length; i++) {
assertEquals(expected[i], arr1[i]);
}



Pipe.confirmLowLevelRead(p, msgsize);
Pipe.releaseReadLock(p);
Pipe.confirmLowLevelRead(p1, msgsize);
Pipe.releaseReadLock(p1);

}

@Test
public void validatePipeBlobHasDataToRead(){
	Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); 
p.initBuffers();
int val = Pipe.bytesOfContent(p);

//
assertEquals(0, val);



int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_INT_40);

assertEquals(Pipe.sizeOf(TestDataSchema.instance, TestDataSchema.MSG_INT_40), msgsize);
Pipe.addIntValue(1, p);


Pipe.confirmLowLevelWrite(p, msgsize); 


Pipe.publishWrites(p); 

//System.out.println(p.blobWriteLastConsumedPos);
boolean bool = Pipe.validatePipeBlobHasDataToRead(p, 0, 1);

assertTrue(!bool); //negative test case. only writte to in case of string

int msgidx = Pipe.takeMsgIdx(p);
assertEquals(msgidx, TestDataSchema.MSG_INT_40); 

int val1 = Pipe.takeInt(p);

assertEquals(1, val1); 

Pipe.confirmLowLevelRead(p, msgsize);
Pipe.releaseReadLock(p);


 Pipe.addMsgIdx(p, TestDataSchema.MSG_CHUNKEDSTREAM_10);
 Pipe.addASCII("test", p);
 
 int meta = Pipe.takeRingByteMetaData(p); //just assume this is correct.
	int length    = Math.max(0, Pipe.takeRingByteLen(p));
	  int position = Pipe.bytePosition(meta, p, length);
 
 bool = Pipe.validatePipeBlobHasDataToRead(p, position, length); //tests to see if string field is in blob
 assertTrue(bool);
 
 
 Pipe.releaseReadLock(p);
	
}

//need to sit down for like an hour and go through the aspects fo the test that are not behaing the way they should be.


@Test
public void hasRoomForWritetest(){
	Pipe<TestDataSchema> p = TestDataSchema.instance.newPipe(4, 100); 
	p.initBuffers();
	int val = Pipe.bytesOfContent(p);


	assertEquals(0, val);

	int msgsize = Pipe.addMsgIdx(p, TestDataSchema.MSG_INT_40);
	
	boolean hasRoom = Pipe.hasRoomForWrite(p);

assertTrue(hasRoom);
	 hasRoom = Pipe.hasRoomForWrite(p,1);
	
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