package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.WrappedPlainView;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.parse.ByteConsumerCodePointConverter;

public class TrieParserTest {

	byte[] data1 = new byte[]{101,102,103,104,105,106,107,108};

	byte[] data2  = new byte[]{106,107,108,109,110,111,112,113};
	byte[] data2b = new byte[]{106,107,108,109,110,111,118,119};
	byte[] data3  = new byte[]{106,107,108,109,120,121,122,123};
	byte[] data3b = new byte[]{106,107,108,109,120,121,(byte)128,(byte)129};

	byte[] data4 = new byte[]{106,107,108,109,(byte)130,(byte)131,(byte)132,(byte)133};

	byte[] data5 = new byte[]{106,117,118,119,110,111,112,113};

	int value1 = 10;
	int value2 = 23;
	int value3 = 35;
	int value4 = 47;
	int value5 = 51;
	int value6 = 69;
	int value7 = 72;

	int value8 = 91;
	int value9 = 93;


	byte[] escapedEscape = new byte[]{100,101,102,'%','%',127};


	//examples all end with extract
	byte[] dataBytesExtractEnd  = new byte[]{100,101,102,'%','b',127};
	byte[] dataBytesExtractEnd2 = new byte[]{100,101,102,'%','b',127,102};
	byte[] dataBytesExtractEnd3 = new byte[]{100,101,102,'%','b',125};
	byte[] dataBytesExtractEnd4 = new byte[]{100,101,102,'%','b',126};
	byte[] dataBytesExtractEndA = new byte[]{100,101,102,'A','b',127};
	byte[] dataBytesExtractEndB = new byte[]{100,101,102,'B','b',127};
	byte[] dataBytesExtractEndC = new byte[]{100,101,102,'C','b',127};

	//examples all start with extract
	byte[] dataBytesExtractStart  = new byte[]{'%','b',127,100,101,102};
	byte[] dataBytesExtractStart2 = new byte[]{'%','b',127,102,101,102};
	byte[] dataBytesExtractStart3 = new byte[]{'%','b',125,100,101,102};
	byte[] dataBytesExtractStart4 = new byte[]{'%','b',126,100,101,102};
	byte[] dataBytesExtractStartA = new byte[]{'A','b',127,100,101,102};
	byte[] dataBytesExtractStartB = new byte[]{'B','b',127,100,101,102};
	byte[] dataBytesExtractStartC = new byte[]{'C','b',127,100,101,102};

	byte[] toParseStart  = new byte[]{10,20,30,127,100,101,102,111}; //start
	byte[] toParseStart2 = new byte[]{10,20,30,127,102,101,102};    //start2
	byte[] toParseStart3 = new byte[]{10,20,30,125,100,101,102};     //start3
	byte[] toParseStartx = new byte[]{10,20,30,125,100,111,111};     //startx




	byte[] dataBytesExtractMiddle = new byte[]{100,101,'%','b',127, 102};
	byte[] dataBytesExtractBeginning = new byte[]{'%','b',127,100,101,102};

	byte[] toParseEnd       = new byte[]{100,101,102,10,11,12,13,127};
	byte[] toParseEndCopy       = new byte[]{98,99,100,101,102,10,11,12,13,127,102};
	byte[] toParseEnd3      = new byte[]{100,101,102,10,11,12,13,125};
	byte[] toParseEnd4      = new byte[]{100,101,102,10,11,12,13,126};    

	byte[] toParseMiddle    = new byte[]{100,101,10,11,12,13,127,102};
	byte[] toParseMiddleCopy    = new byte[]{100,101,102,10,11,12,13,127,102};
	byte[] toParseBeginning = new byte[]{10,11,12,13,127,100,101,102};


	//test for byte extract followed by different tails
	byte[] dataBytesMultiBytes1 = new byte[]{100,102,'%','b','\r','\n',0,0};//wraps
	byte[] dataBytesMultiBytes2 = new byte[]{100,103,'%','b','\r','\n'};
	byte[] dataBytesMultiBytes3 = new byte[]{100,102,'%','b','\n',0,0,0};//wraps

	byte[] dataBytesMultiBytesValue1 = new byte[]{100,102,10,11,12,'\r','\n'};//xxxxxxxx
	byte[] dataBytesMultiBytesValue2 = new byte[]{100,103,20,21,22,23,'\r','\n'};
	byte[] dataBytesMultiBytesValue3 = new byte[]{100,103,30,31,'\r','\n'};


	//test examples assumed initially
	byte[] data_catalog = new byte[]{99,97,116,97,108,111,103};
	byte[] data_cat_p_b = new byte[]{99,97,116,'%','b'};
	byte[] data_catalyst = new byte[]{99,97,116,97,108,121,115,116};

	
	@Test
	public void testCharSequenceQuery(){
		
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		CharSequence test = "hello";
		CharSequence test1 = "1234";
		map.setUTF8Value("hello", 2);
		map.setUTF8Value("%b", 3);
		assertEquals(2, reader.query(map, test));
		assertEquals(3, reader.query(map, test1));//any value will map to 3 as wildcard.
	}
	//********************************************
//	@Test
//	public void testBlobQuery(){ //TrieParserReader reader, TrieParser trie, CharSequence cs
//		
//		TrieParserReader reader = new TrieParserReader(3);
//		TrieParser map = new TrieParser(16);
//		CharSequence test = "hello";
//		CharSequence test1 = "1234";
//		map.setUTF8Value("hello", 2);
//		map.setUTF8Value("%b", 3);
//		TrieParserReader.blobQueryPrep(reader);
//	//	assertEquals(2, TrieParserReader.blobQuery(reader,map, test));
//		assertEquals(3, TrieParserReader.blobQuery(reader, map, test1));//any value will map to 3 as wildcard.
//	}
	
	@Test//******
	//captured val: whatever is in wild card.
	public void testwriteCapturedValuesToAppendable() throws IOException{
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		//map.setValue(wrapping(dataBytesExtractStart,4), 0, dataBytesExtractStart.length, 15, value2); 
		map.setUTF8Value("%b1234", 33);
		StringBuilder x = new StringBuilder();
		

		
		CharSequence test = "abcd1234";
		reader.query(map, test); //query holds most recent thing (printing query call to console gives number matched).
		// captured wild card should be abcd
		TrieParserReader.writeCapturedValuesToAppendable(reader, x);
		
		
		assertEquals("[4]abcd", x.toString()); //format of String will be [Length]characterscaptured 
	//now test No captured vals
		x = new StringBuilder();
		CharSequence test1 = "1234";
		reader.query(map, test1); //query holds most recent thing (printing query call to console gives number matched).
		// captured wild card should be abcd
		TrieParserReader.writeCapturedValuesToAppendable(reader, x);

		
		assertEquals("[0]", x.toString()); //format of String will be [Length]characterscaptured 
	
	}

	
	@Test
	public void testwriteCapturedValuesToDataOutput() throws IOException{
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		//map.setValue(wrapping(dataBytesExtractStart,4), 0, dataBytesExtractStart.length, 15, value2); 
		map.setUTF8Value("%b1234", 33);
		CharSequence test1 = "abcd1234";
		reader.query(map, test1);
		
		Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(2, 64);
		pipe.initBuffers();
		
		int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter x =Pipe.outputStream(pipe);
		DataOutputBlobWriter.openField(x);
		
		//write something
		int numCapturedBytes = TrieParserReader.writeCapturedValuesToDataOutput(reader, x, true); // -> should equal 4 from above ex.
		
		x.closeLowLevelField();
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		///////////
		int msg = Pipe.takeMsgIdx(pipe);
		assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, msg);
		//String value = Pipe.takeUTF8(pipe); //something with UTF in method name
		DataInputBlobReader y = Pipe.inputStream(pipe);
y.openLowLevelAPIField();
StringBuilder str = new StringBuilder();
y.readUTFOfLength(y.available(), str);
		
		//assert("safdsfasdf", value);
		Pipe.confirmLowLevelRead(pipe, size);
		Pipe.releaseReadLock(pipe);
		
		assertEquals(numCapturedBytes, 4);
		
		
		
	//test to cover unsigned decimal value.
		
		 reader = new TrieParserReader(3);
		 map = new TrieParser(16);
		
		map.setUTF8Value("%i%.", 33);
		String test2 = "3.75";
		reader.query(map, test2);
		
		 pipe = RawDataSchema.instance.newPipe(2, 64);
		pipe.initBuffers();
		
		 size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		 x =Pipe.outputStream(pipe);
		DataOutputBlobWriter.openField(x);
		
		//write something
		 numCapturedBytes = TrieParserReader.writeCapturedValuesToDataOutput(reader, x, true); // -> should equal 4 from above ex.
		
		x.closeLowLevelField();
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		///////////
		 msg = Pipe.takeMsgIdx(pipe);
		assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, msg);
		
		 y = Pipe.inputStream(pipe);
y.openLowLevelAPIField();

	long l1 = y.readPackedLong(); //this will return 375 for "3.75
	int b1 =  y.read(); //this will give us a -2, to tell us where the decimal should go.
	
	
	
		
		Pipe.confirmLowLevelRead(pipe, size);
		Pipe.releaseReadLock(pipe);
		
		
		
		assertEquals(l1, 375);
		assertEquals(b1, -2);
	}
	
	@Test
	public void testcapturedFieldBytesAsUTF8Debug(){
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("%b1234", 33);
		StringBuilder x = new StringBuilder();
		
		CharSequence test = "abcd1234";
		reader.query(map, test); //query holds most recent thing (printing query call to console gives number matched).
		
		x = TrieParserReader.capturedFieldBytesAsUTF8Debug(reader,0, x);
		//System.out.println(x.toString());
		String y = x.toString().trim(); //y = abcd1234 with garbage vals
		assertEquals(10,x.indexOf((String) test) );  //actual value starts after 10 garbage vals
		assertTrue(x.length()>=test.length()+10 ); //only captures first 10 garbage values for some reason
		
	}
	
	@Test

	public void testcapturedFieldSetValue(){
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b1234", 33);
	
		StringBuilder x = new StringBuilder();
		
		CharSequence test = "12abcd1234";
		reader.query(map, test); //query holds most recent thing 
		//maps abcd(captured value) to new val of 22.
		TrieParserReader.capturedFieldSetValue(reader, 0, map, 22);
long val = reader.query(map, "abcd");

		assertEquals(val,22);
	}
	
	@Test
	public void testcapturedFieldQuery(){
		//*****method parses the capture text as a query against yet another trie
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		TrieParser map2 = new TrieParser(16);
		map.setUTF8Value("%b1234", 33);
	map2.setUTF8Value("abcd", 12); //second trie to test captured val of first map1 query

		
		
		 reader.query(map, "abcd1234");
long val1 = TrieParserReader.capturedFieldQuery(reader,0,map2);//searches map2 for saved query from map1(abcd) which is mapped to 12 in map2
assertEquals(val1,12);
	}
	
	

	@Test
	public void testcapturedFieldBytes(){
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b1234", 33);
	
		
		
		CharSequence test = "12abcd1234";
		reader.query(map, test); //query holds most recent thing 
		
		final StringBuilder str = new StringBuilder();
		
		ByteConsumer byteconsumer = new ByteConsumer() {
			
			@Override
			public void consume(byte[] backing, int pos, int len, int mask) {				
		
			//this will append the captured values to stringBuilder(outside anon class) as they are being 'consumed'
				
				for(int i = 0;i<len;i++){
				str.append((char)backing[mask & pos++]);
			}
			}

			@Override
			public void consume(byte value) {				
			}};
		//same as Byte length method below?? should I be doing something in
		//unimplemennted consume methods below?
		int x = TrieParserReader.capturedFieldBytes(reader,0,byteconsumer);//Null point excep
		
		int capturedbytelength = TrieParserReader.capturedFieldBytesLength(reader, 0);
		
		assertEquals(capturedbytelength,4);
		assertEquals(x,4);
		assertEquals(str.toString(),"abcd");
	}
	
	@Test
	public void testwriteCapturedShort(){
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%iabcd", 33);
	
		
		
		CharSequence test = "128abcd";
		reader.query(map, test); //query holds most recent thing 
		Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(2, 64);
		pipe.initBuffers();
		
		int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter x =Pipe.outputStream(pipe);
		DataOutputBlobWriter.openField(x);
		
		
		 TrieParserReader.writeCapturedShort(reader, 0, x);
		
		 
		 x.closeLowLevelField();
			Pipe.confirmLowLevelWrite(pipe, size);
			Pipe.publishWrites(pipe);
			///////////
			int msg = Pipe.takeMsgIdx(pipe);
			assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, msg);
		
			DataInputBlobReader y = Pipe.inputStream(pipe);
	y.openLowLevelAPIField();
	StringBuilder str = new StringBuilder();
	//System.out.println(y.readLine());
	//2nd value has length of captured field.
	byte[] vals = new byte[2];
	y.read(vals);
	
	
	
	assertEquals(vals[1],8);
	}
	
	
	@Test
	public void testDebug(){
		//should I be testing debugging methods? the sys.err? ask tom.
		
		//in here we will test debug() and debugAsUTF8.
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b12", 33);
	
		
		
		CharSequence test = "12abcd12";
		long val = reader.query(map, test);
	TrieParserReader.parseSetup(reader, "12abcd12".getBytes(),0,8,7);
//get output from system.err console and save as string
		ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
		PrintStream PS = new PrintStream(bytestream);
		PrintStream old = System.out;

		System.setErr(PS);
		reader.debug();
		//System.out.println( bytestream.toString());

		String result = bytestream.toString();
		//regex to parse the int values from debugger to check that they are correct. could prob be simpler.
		Pattern p = Pattern.compile("-?\\d+");
		Matcher m = p.matcher(result);
		
		String result_string = "";
		while (m.find()) {
		// n[i] = Integer.parseInt(m.group(i++));
			result_string += m.group();
		}
		System.out.println(result_string);
		
		assertEquals(result_string.charAt(0),'0'); //compare 'pos'
		assertEquals(result_string.charAt(1),'0'); //mask
		assertEquals(result_string.charAt(3),'7'); //
		assertEquals(result_string.charAt(4),'8');
	
	}
	
	
	
	
	@Test public void testdebugAsUTF8(){
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b12", 33);
	
		
		
		CharSequence test = "12abcd12";
		TrieParserReader.parseSetup(reader, "12abcd12".getBytes(),0,8,7);
		long val = reader.query(map, test);
		StringBuilder str = new StringBuilder();

		int result = reader.debugAsUTF8(reader, str);
	
		System.out.println(result);
		assertEquals(result,8);
		
	}
	
	
	@Test
	public void testparseGather(){
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b12", 33);
	
		
		
		CharSequence test = "12abcd12";
		TrieParserReader.parseSetup(reader, "12abcd12".getBytes(),0,8,7);
		long val = reader.query(map, test);
		Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(2, 64);
		pipe.initBuffers();
		
		int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter x =Pipe.outputStream(pipe);
		DataOutputBlobWriter.openField(x);
		
	
		reader.parseGather(reader, x,(byte) 'd'); // so will give 5. 5 bytes until d is hit. if i put 'c' will return length of 4.
	System.out.println(reader.sourcePos);
		
		x.closeLowLevelField();
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		///////////
		int msg = Pipe.takeMsgIdx(pipe);
		assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, msg);
	
		DataInputBlobReader y = Pipe.inputStream(pipe);
y.openLowLevelAPIField();
StringBuilder str = new StringBuilder();
y.readUTFOfLength(y.available(), str);


//dont understand what this parseGather() is supposed to do.
reader.parseGather( reader, (byte) 'd');
//just changes sourcePos? but this shouldne be 22 if my sixe of source array is only like 8
System.out.println(reader.sourcePos);
assertEquals(str.length(),5);
assertEquals(str.toString(),"12abc");
assertEquals(22,reader.sourcePos);
	
		

	}
	
	
	
	@Test
	public void testparseSkip(){
		//will test all parse skip(and parseskipone) methods.
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b123", 33);
	
		
		
		CharSequence test = "12abcd123";
		// TrieParserReader that, byte[] source, int offset, int length, int mask
		TrieParserReader.parseSetup(reader,"12abcd123".getBytes(),0,9,8);
		assertEquals(9,reader.sourceLen); 
		int x = reader.parseSkipOne();
		int len = reader.parseSkip(2);
		
		long val = reader.query(map, test);
		
	
		assertEquals(x,49); //ascii for 1.
		assertEquals(val,33);
		assertEquals(len,2); //length of skip(same as parameter essentially.
		
reader = new TrieParserReader(3); 
TrieParserReader.parseSetup(reader,"12abcd123".getBytes(),0,9,8);
boolean b = TrieParserReader.parseSkipUntil(reader, 51); //51 is ascii for 3. will match 3 and return true.

 val = reader.query(map, test);
assertTrue(b);
	}

	
	@Test
	public void testparseCopy(){
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b12", 33);
	
		
		
		CharSequence test = "12abcd12";
		// TrieParserReader that, byte[] source, int offset, int length, int mask
		TrieParserReader.parseSetup(reader,"12%b12".getBytes(),0,6,7);
		assertEquals(6,reader.sourceLen); 
		
		
		Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(2, 64);
		pipe.initBuffers();
		
		int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter x =Pipe.outputStream(pipe);
		DataOutputBlobWriter.openField(x);
		
		
		int val = TrieParserReader.parseCopy( reader, 6, x); //will copy the value in mapping to dataoutputblobwriter

		x.closeLowLevelField();
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		///////////
		int msg = Pipe.takeMsgIdx(pipe);
		assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, msg);
	
		DataInputBlobReader y = Pipe.inputStream(pipe);
y.openLowLevelAPIField();
StringBuilder str = new StringBuilder();
y.readUTFOfLength(y.available(), str);

		
assertEquals(str.toString(),"12%b12");
assertEquals(val,6); //asserting length returned by parseCopy is length given. would not be the case if sourcelength was greater.
	}
	
	
	//**********
	// figure out what this does
	@Test
	public void testsaveloadPositionMemo(){
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b", 33);
	
		
		
		CharSequence test = "12abcd";
		// TrieParserReader that, byte[] source, int offset, int length, int mask
		TrieParserReader.parseSetup(reader,"12%b".getBytes(),0,4,7);
		long val = TrieParserReader.query(reader, map, test);
		TrieParserReader.parseSetup(reader,"12%b".getBytes(),0,4,7);
		int[] target = {49,50,61,62,63,64};//holds all vals of char sequence?
		int value = TrieParserReader.savePositionMemo(reader, target, 0);
	
//target will be [61,62,63,64] -> saved captured vals.
		
		System.out.println("val " + val);
		System.out.println("value: " + value);
		assertEquals(val,33); //just standard query test, make sure parsetSetup is correct.
		assertEquals(value,4); // length of captured target array from savePositionMemo();
	}
	
	@Test 
	public void testblobQuery() throws IOException{
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b12", 33);
	//TrieParserReader reader, TrieParser trie, 
       // byte[] source, int sourcePos, long sourceLength, int sourceMask, 
        //final long unfoundResult) {
	long no_val = TrieParserReader.query(reader,map, "NoMatchonlong".getBytes(),0,"No Match on long test.".length(),15,-1);	
		System.out.println("NOVAL " + no_val);
		CharSequence test = "12abcd12";
		
		long val = reader.query(map, test);
		System.out.println(val);
		
		//below is testing cs.length()> reader.workingPipe.maxVarLen in Query() mthod
		StringBuilder maxlengthtest = new StringBuilder();
		
		for(int i = 0; i<2000;i++){
			maxlengthtest.append(i);
		}
		
		long val1 = reader.query(map, maxlengthtest.toString());
		
//		Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(2, 64);
//		pipe.initBuffers();
//		
//		int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
//		DataOutputBlobWriter x =Pipe.outputStream(pipe);
//		DataOutputBlobWriter.openField(x);
//		
	
		//reader.parseGather(reader, x,(byte) 'd'); // so will give 5. 5 bytes until d is hit. if i put 'c' will return length of 4.

		
		ChannelWriter x = reader.blobQueryPrep(reader);
		x.append(test); 
	long yy = reader.blobQuery(reader, map);
	
	System.out.println(x.length());

		System.out.println(yy);
		x.close();

assertEquals(yy,33); //blobquery will return the mapping of the charsequence in the map
		
	}
	@Test
	public void testwriteCapturedUTF8(){
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		
		map.setUTF8Value("12%b1234", 33);
	
		
		
		CharSequence test = "12abcd1234";
		reader.query(map, test); //query holds most recent thing 
		Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(2, 64);
		pipe.initBuffers();
		
		int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter x =Pipe.outputStream(pipe);
		DataOutputBlobWriter.openField(x);
		
		//will append "abcd" to blobWriter
		int val = TrieParserReader.writeCapturedUTF8(reader,0,x);
	
		
		
		x.closeLowLevelField();
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		///////////
		int msg = Pipe.takeMsgIdx(pipe);
		assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, msg);
	
		DataInputBlobReader y = Pipe.inputStream(pipe);
y.openLowLevelAPIField();
StringBuilder str = new StringBuilder();
y.readUTFOfLength(y.available(), str);


 //value of blobwriter sent to stringbuilder to sompare with other string
assertEquals(str.toString().trim(),"abcd");
		
	}
	
	@Ignore
	public void testwriteCapturedUTF8ToPipe() throws IOException{
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		//map.setValue(wrapping(dataBytesExtractStart,4), 0, dataBytesExtractStart.length, 15, value2); 
		map.setUTF8Value("%b1234", 33);
		CharSequence test1 = "abcd1234";
		reader.query(map, test1);
		
		Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(2, 64);
		pipe.initBuffers();
		
		int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter x =Pipe.outputStream(pipe);
		DataOutputBlobWriter.openField(x);
		
//just like in test above, will return the number of captured bytes 4 in this case(abcd).
		int num = TrieParserReader.writeCapturedUTF8ToPipe(reader, pipe, 0, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);


		x.closeLowLevelField();
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		///////////
		int msg = Pipe.takeMsgIdx(pipe);
		assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, msg);
	
		DataInputBlobReader y = Pipe.inputStream(pipe);
y.openLowLevelAPIField();
StringBuilder str = new StringBuilder();
y.readUTFOfLength(y.available(), str);
		
		
		Pipe.confirmLowLevelRead(pipe, size);
		Pipe.releaseReadLock(pipe);
		
		assertEquals(num, 4);
		
	}
	
	
	
	
	@Test 
	public void testExtractMultiBytes() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);


		map.setValue(toParseEnd, 0, toParseEnd.length, 15, value4);
		map.setValue(toParseMiddle, 0, toParseMiddle.length, 15, value4);
		map.setValue(toParseBeginning, 0, toParseBeginning.length, 15, value4);        
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(dataBytesMultiBytes1, 0, 6, 7, value1);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value1, TrieParserReader.query(reader,map,dataBytesMultiBytesValue1, 0, dataBytesMultiBytesValue1.length, 15));

		map.setValue(dataBytesMultiBytes2, 0, dataBytesMultiBytes2.length, 15, value2);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value2, TrieParserReader.query(reader,map,dataBytesMultiBytesValue2, 0, dataBytesMultiBytesValue2.length, 15));

		map.setValue(dataBytesMultiBytes3, 0, 5, 7, value3); //the /n is added last it takes priority and gets selected below.
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		//NOTE: that %b\n is shorter and 'simpler; than %b\r\n so the first is chosen and the \r becomes part of the captured data.
		assertEquals(value3, TrieParserReader.query(reader,map,dataBytesMultiBytesValue1, 0, dataBytesMultiBytesValue1.length, 15));
		assertEquals(value2, TrieParserReader.query(reader,map,dataBytesMultiBytesValue2, 0, dataBytesMultiBytesValue2.length, 15));
		assertEquals(value2, TrieParserReader.query(reader,map,dataBytesMultiBytesValue3, 0, dataBytesMultiBytesValue3.length, 15));

	}

	@Test 
	public void testExtractMultiBytes2() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(1000);

		map.setValue(toParseEnd, 0, toParseEnd.length, 15, value4);
		map.setValue(toParseMiddle, 0, toParseMiddle.length, 15, value4);
		map.setValue(toParseBeginning, 0, toParseBeginning.length, 15, value4);        

		assertFalse(map.toString(),map.toString().contains("ERROR"));     

		map.setValue(dataBytesMultiBytes3, 0, dataBytesMultiBytes3.length, 15, value3);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(dataBytesMultiBytes1, 0, 6, 7, value1);
		assertFalse(map.toString(),map.toString().contains("ERROR"));
	}

	@Test 
	public void testQuotesWithExtractions() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(1000);

		map.setUTF8Value("\"%b\"", 1);
		map.setUTF8Value("\"%b\\", 2);

		//match first case since it ends in quote
		byte[] testMatch1 = "\"hello\"             ".getBytes();        
		assertEquals(1, TrieParserReader.query(reader,map,testMatch1, 0, testMatch1.length, 7));

		//match second case since it ends in slash
		byte[] testMatch2 = "\"hello\\             ".getBytes();        
		assertEquals(2, TrieParserReader.query(reader,map,testMatch2, 0, testMatch2.length, 7));

		//no match because it does not end with the right char
		byte[] testMatch3 = "\"hello          ".getBytes();        
		assertEquals(-1, TrieParserReader.query(reader,map,testMatch3, 0, testMatch3.length, 15));


	}

	@Test 
	public void testExtractBytesEnd() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(data1, 0, 3, 7, value1); //101,102,103
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd,3), 0, dataBytesExtractEnd.length, 7, value2);
		assertFalse(map.toString(),map.toString().contains("ERROR")); 

		map.setValue(data1, 2, 3, 7, value3); //103,104,105
		assertFalse(map.toString(),map.toString().contains("ERROR")); 

		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 3, 7)); // 101,102,103
		assertEquals(value3, TrieParserReader.query(reader,map,data1, 2, 3, 7)); // 103,104,105


		assertEquals(value2, TrieParserReader.query(reader,map,toParseEnd, 0, toParseEnd.length, 7));

		assertEquals(1, TrieParserReader.capturedFieldCount(reader));

		byte[] expected = new byte[]{0,0,0,0};        
		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(expected) );

	}

	@Test 
	public void testExtractBytesEnd2a() {
		TrieParserReader reader = new TrieParserReader(3,true);
		TrieParser map = new TrieParser(16);

		map.setValue(data1, 0, 3, 7, value1);

		map.setValue(wrapping(dataBytesExtractEnd2,3), 0, dataBytesExtractEnd2.length, 7, value4); //100,101,102,'%','b',127,102
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd,3), 0, dataBytesExtractEnd.length, 7, value2);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 2, 3, 7, value3);        
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 3, 7));
		assertEquals(value3, TrieParserReader.query(reader,map,data1, 2, 3, 7));

		//100,101,10,11,12,13,127,102
		assertEquals(-1, TrieParserReader.query(reader,map, wrapping(toParseMiddle,4), 0, toParseMiddle.length, 15));
		
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(toParseEnd,4), 0, toParseEnd.length, 15));

		assertEquals(1, TrieParserReader.capturedFieldCount(reader));

		byte[] expected = new byte[]{0,0,0,0};        
		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(expected) );

	}

	@Test 
	public void testExtractBytesEnd2b() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(wrapping(data1,4), 0, 3, 15, value1);                                          //1  added  101,102,103
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd,4), 0, dataBytesExtractEnd.length, 15, value2);   //2  added  100,101,102,'%','b',127
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd2,4), 0, dataBytesExtractEnd2.length, 15, value4); //4  added  100,101,102,'%','b',127,102
		assertFalse("\n"+map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 2, 3, 7, value3);                                                       //3  added  103,104,105

		assertEquals(value1, TrieParserReader.query(reader, map, wrapping(data1,4), 0, 3, 15));

		assertEquals(value3, TrieParserReader.query(reader, map, wrapping(data1,4), 2, 3, 15));

		//100,101,10,11,12,13,127,102
		assertEquals(-1, TrieParserReader.query(reader, map, wrapping(toParseMiddle,4), 0, toParseMiddle.length, 15));
		assertEquals(value2, TrieParserReader.query(reader, map, wrapping(toParseEnd,4), 0, toParseEnd.length, 15));

		assertEquals(1, TrieParserReader.capturedFieldCount(reader));

		byte[] expected = new byte[]{0,0,0,0};        
		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(expected) );

	}

	@Test 
	public void testExtractBytesEndAll() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16,false);

		map.setValue(wrapping(data1,4), 0, 3, 15, value1);                                          //added  101,102,103
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEndA,4), 0, dataBytesExtractEndA.length, 15, value5); //added  100,101,102,'A','b',127 

		map.setValue(wrapping(dataBytesExtractEnd,4), 0, dataBytesExtractEnd.length, 15, value2);   //added  100,101,102,'%','b',127
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEndB,4), 0, dataBytesExtractEndB.length, 15, value6); //added  100,101,102,'B','b',127 

		map.setValue(wrapping(dataBytesExtractEnd2,4), 0, dataBytesExtractEnd2.length, 15, value4); //added  100,101,102,'%','b',127,102
		assertFalse("\n"+map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEndC,4), 0, dataBytesExtractEndC.length, 15, value7); //added   100,101,102,'C','b',127 

		map.setValue(data1, 2, 3, 7, value3);  //    101,102,[ 103,104,105, ] 106,107,108

		assertEquals(value1, TrieParserReader.query(reader, map, wrapping(data1,4), 0, 3, 15));

		assertEquals(value3, TrieParserReader.query(reader, map, wrapping(data1,4), 2, 3, 15));

		// {100,101,10,11,12,13,127,102};
		assertEquals(-1, TrieParserReader.query(reader, map, wrapping(toParseMiddle,4), 0, toParseMiddle.length, 15));

		// {100,101,102,10,11,12,13,127}
		assertEquals(value2, TrieParserReader.query(reader, map, wrapping(toParseEnd,4), 0, toParseEnd.length, 15));

		assertEquals(1, TrieParserReader.capturedFieldCount(reader));
		byte[] expected = new byte[]{0,0,0,0};        
		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(expected) );

		assertEquals(value5, TrieParserReader.query(reader, map, wrapping(dataBytesExtractEndA,4), 0, dataBytesExtractEndA.length, 15));
		assertEquals(value6, TrieParserReader.query(reader, map, wrapping(dataBytesExtractEndB,4), 0, dataBytesExtractEndB.length, 15));
		assertEquals(value7, TrieParserReader.query(reader, map, wrapping(dataBytesExtractEndC,4), 0, dataBytesExtractEndC.length, 15));

	}



	@Test 
	public void testExtractBytesEndStart() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(1000,false);

		map.setValue(wrapping(data1,4), 0, 3, 15, value1);                                              //101,102,103 e,f,g                                   
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractStart,4), 0, dataBytesExtractStart.length, 15, value2);  //{'%','b',127,100,101,102}; //def
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		//map.toDOT(System.out);

		map.setValue(wrapping(dataBytesExtractStart2,4), 0, dataBytesExtractStart2.length, 15, value4);//{'%','b',127,102,101,102}; //fef
		assertFalse(map.toString(),map.toString().contains("ERROR"));
		//map.toDOT(System.out);

		map.setValue(wrapping(dataBytesExtractStart3,4), 0, dataBytesExtractStart3.length, 15, value1);//{'%','b',125,100,101,102};
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractStartA,4), 0, dataBytesExtractStartA.length, 15, value5); //'A','b',127,100,101,102 
		map.setValue(wrapping(dataBytesExtractStartB,4), 0, dataBytesExtractStartB.length, 15, value6);  


		assertFalse("\n"+map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractStartC,4), 0, dataBytesExtractStartC.length, 15, value7);  

		map.setValue(data1, 2, 3, 7, value3); //103,104,105

		assertEquals(value1, TrieParserReader.query(reader, map, wrapping(data1,4), 0, 3, 15));

		assertEquals(value3, TrieParserReader.query(reader, map, wrapping(data1,4), 2, 3, 15)); // 103,104,105

		assertEquals(value5, TrieParserReader.query(reader, map, wrapping(dataBytesExtractStartA,4), 0, dataBytesExtractStartA.length, 15));
		assertEquals(value6, TrieParserReader.query(reader, map, wrapping(dataBytesExtractStartB,4), 0, dataBytesExtractStartB.length, 15));
		assertEquals(value7, TrieParserReader.query(reader, map, wrapping(dataBytesExtractStartC,4), 0, dataBytesExtractStartC.length, 15));

		assertEquals(value2, TrieParserReader.query(reader, map, wrapping(toParseStart,4), 0, toParseStart.length, 15));  //{10,20,30,127,100,101,102,155}; //start


		assertEquals(value1, TrieParserReader.query(reader, map, wrapping(toParseStart3,4), 0, toParseStart3.length, 15));  //{10,20,30,125,100,101,102};     //start3

		//map.toDOT(System.out); //this is a pretty example showing the ALTs on the left and the explicit paths on the right
		assertEquals(-1, TrieParserReader.query(reader, map, wrapping(toParseStartx,4), 0, toParseStartx.length, 15));      //{10,20,30,125,100,155,155};     //startx

		assertEquals(value4, TrieParserReader.query(reader, map, wrapping(toParseStart2,4), 0, toParseStart2.length, 15));  //{10,20,30,127,100,102,101,102}; //start2





	}



	/**
	 * Extract has multiple end points all determined by last stop byte.
	 */
	@Test 
	public void testExtractBytesEndMulti() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(1000);

		map.setValue(data1, 0, 3, 7, value1); //101,102,103
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd,3), 0, dataBytesExtractEnd.length, 7, value2);
		map.setValue(wrapping(dataBytesExtractEnd3,3), 0, dataBytesExtractEnd3.length, 7, value3);
		map.setValue(wrapping(dataBytesExtractEnd4,3), 0, dataBytesExtractEnd4.length, 7, value4);

		assertFalse(map.toString(),map.toString().contains("ERROR")); 

		map.setValue(data1, 2, 3, 7, value3); //103,104,105
		assertFalse(map.toString(),map.toString().contains("ERROR")); 

		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 3, 7)); // 101,102,103
		assertEquals(value3, TrieParserReader.query(reader,map,data1, 2, 3, 7)); // 103,104,105

		//TODO: NOTE: this test works however it runs each of the captures to the end and returns the first but what we really want is a single pass returning the shortest capture.
		//      TODO: to solve the above, before doing byte capture must check the stack for parallel captures and list all the stop nodes. Now stack is checked after failure.

		assertEquals(value2, TrieParserReader.query(reader,map,toParseEnd, 0, toParseEnd.length, 7));

		byte[] expected = new byte[]{0,0,0,0};      

		assertEquals(1, TrieParserReader.capturedFieldCount(reader));

		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(expected) );

		assertEquals(value3, TrieParserReader.query(reader,map,toParseEnd3, 0, toParseEnd3.length, 7));

		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(expected) );

		assertEquals(value4, TrieParserReader.query(reader,map,toParseEnd4, 0, toParseEnd3.length, 7));

		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(expected) );

	}


	@Test
	public void testNonBranchInsert() {

		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		map.setUTF8Value("Hello: %u\r",   value2); //FYI, one should not see one of these in the wild often.
		map.setUTF8Value("Hello: %u\r\n", value3); //This just ends later so there is no branch 

		assertFalse(map.toString().contains("BRANCH_VALUE1"));

		byte[] text1 = "Hello: 123\r".getBytes();
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(text1,4), 0, text1.length, 15));

		byte[] text2 = "Hello: 123\r\n".getBytes();
		assertEquals(value3, TrieParserReader.query(reader,map, wrapping(text2,4), 0, text2.length, 15));

	}

	@Test
	public void testNumberAtEnd() {

		TrieParserReader reader = new TrieParserReader(2,true);
		TrieParser map = new TrieParser(16);
		map.setValue("unfollow/%u".getBytes(),0,"unfollow/%u".length(),Integer.MAX_VALUE, value2);

		byte[] pat = map.lastSetValueExtractonPattern();
		assertEquals(1,pat.length);
		assertEquals(TrieParser.ESCAPE_CMD_UNSIGNED_INT, pat[0]);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		byte[] text1 = "unfollow/61426357200000".getBytes();
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(text1,6), 0, text1.length, 63));
        long value = TrieParserReader.capturedLongField(reader, 0);
        assertEquals(61426357200000L,value);
		
		
		byte[] text2 = "unfollow/%u".getBytes();
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(text2,4), 0, text2.length, 15));

		byte[] text3 = "unfollow/123]".getBytes();
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(text3,4), 0, text3.length-1, 15));

		
	}

	@Test
	public void testSimpleURLPaths() {

		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16,false);
		map.setUTF8Value("/unfollow?user=%u",   value2);
		map.setUTF8Value("/%b", value3); 

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		byte[] text3 = "No root".getBytes();
		assertEquals(-1, TrieParserReader.query(reader,map, wrapping(text3,5), 0, text3.length, 31));

		byte[] text1 = "/unfollow?user=1234x".getBytes();
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(text1,5), 0, text1.length, 31));

		byte[] text2 = "/Hello: 123\r\n".getBytes();
		assertEquals(value3, TrieParserReader.query(reader,map, wrapping(text2,5), 0, text2.length, 31));

	}


	@Test
	public void testSimpleURLPathsOnRollover() {

		//if we have 2 gigs to run this test do so 
		if (Runtime.getRuntime().freeMemory() > (2L<<30) ) {
			
			
			TrieParserReader reader = new TrieParserReader(3);
			TrieParser map = new TrieParser(16,false);
			map.setUTF8Value("/unfollow?user=%u",   value2);
			map.setUTF8Value("/%b", value3); 
	
			assertFalse(map.toString(),map.toString().contains("ERROR"));
	
			byte[] text3 = "No root".getBytes();
			assertEquals(-1, TrieParserReader.query(reader,map, wrappingRolledOver(text3,5,30), 30, text3.length, 31));
	
			byte[] text1 = "/unfollow?user=1234x".getBytes();
			assertEquals(value2, TrieParserReader.query(reader,map, wrappingRolledOver(text1,5,30), 30, text1.length, 31));
			assertEquals(value2, TrieParserReader.query(reader,map, wrappingRolledOver(text1,5,30), 30+32, text1.length, 31));
			assertEquals(value2, TrieParserReader.query(reader,map, wrappingRolledOver(text1,5,30), 30+1+Integer.MAX_VALUE, text1.length, 31));
			
	//		[HTTP1xResponseParserStage id:21] INFO com.ociweb.pronghorn.network.http.HTTP1xResponseParserStage - error trieReader pos -2147480662 len 90255 
	//		[HTTP1xResponseParserStage id:21] WARN com.ociweb.pronghorn.network.http.HTTP1xResponseParserStage - 1 looking for HTTP revision but found:
	//		HTTP/1.1 200 OK
	//		Server: GreenLightning
	//		Content-Type: text/graphviz...
			
			byte[] text2 = "/Hello: 123\r\n".getBytes();
			
			int bits = 30;
			int mask = (1<<bits)-1;
			int pos = mask-1;
			assertEquals(value3, TrieParserReader.query(reader,map, wrappingRolledOver(text2,bits,pos), pos, text2.length, mask));
		} else {
			assertTrue(true); //we did not run this one.
		}
		
	}

	@Test
	public void testNumericPatternMatchesPatternDef() {

		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16,false);
		map.setUTF8Value("/unfollow?user=%u",   value2);
		map.setUTF8Value("/%b", value3); 

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		byte[] text0 = "/unfollow?user=%u".getBytes();
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(text0,5), 0, text0.length, 31));
	}

	@Test
	public void testNumericPatternMatchesAtEnd() {

		TrieParserReader reader = new TrieParserReader(3, true);
		TrieParser map = new TrieParser(16,false);
		map.setUTF8Value("/unfollow?user=%u",   value2);
		map.setUTF8Value("/%b", value3); 

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		byte[] text0 = "/unfollow?user=12345".getBytes();
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(text0,6), 0, text0.length, 63));
	
	}
	
	@Test
	public void testNumericPatternMatchesAtEndNot() {

		TrieParserReader reader = new TrieParserReader(3, false);
		TrieParser map = new TrieParser(16,false);
		map.setUTF8Value("/unfollow?user=%u",   value2);
		map.setUTF8Value("/%b", value3); 

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		byte[] text0 = "/unfollow?user=12345".getBytes();
		assertEquals(-1, TrieParserReader.query(reader,map, wrapping(text0,6), 0, text0.length, 63));
	
	}
	
	@Test
	public void testOrder1Insert() {

		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16, false);
		map.setUTF8Value("%bb\n",   value2); 
		String a = map.toString();

		map.setUTF8Value("ab\n",    value3);  
		String b = map.toString();
		assertFalse(a.equals(b));

		assertFalse(map.toString(),map.toString().contains("ERROR"));

	}


	@Test
	public void testOrder2Insert() {

		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		map.setUTF8Value("ab\n",    value3);  
		map.setUTF8Value("%bb\n",   value2); 
		map.setUTF8Value("bb\n",    value3);     

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		//System.out.println(map.toString()); //TODO: WARN the bb and ab should have come first and wrapped the others? OR alt branch should not insert first.

	}

	@Test
	public void testMultipleTrysOfTrie() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(1000);
		map.setUTF8Value("tuesday",   value2);
		map.setUTF8Value("hello","2", value3);
		map.setUTF8Value("helloworld",value1);
		map.setUTF8Value("X%b","web", value4);
		map.setUTF8Value("%b","web",  value5);

		assertFalse(map.toString(),map.toString().contains("ERROR"));


		byte[] a = "he                             ".getBytes();
		byte[] b = "hello                          ".getBytes();
		byte[] c = "helloworldtuesday              ".getBytes();


		TrieParserReader.parseSetup(reader, a, 0, 2,  15);

		assertEquals(0, reader.sourcePos);
		assertEquals(2, reader.sourceLen);
		assertEquals(2, TrieParserReader.parseHasContentLength(reader));

		assertEquals(-1, TrieParserReader.parseNext(reader, map));

		TrieParserReader.parseSetup(reader, b, 0, 5, 15);

		assertEquals(0, reader.sourcePos);
		assertEquals(5, reader.sourceLen);
		assertEquals(5, TrieParserReader.parseHasContentLength(reader));

		assertEquals(-1, TrieParserReader.parseNext(reader, map));


		TrieParserReader.parseSetup(reader, c, 0, "helloworld".length(), 31);

		assertEquals(0, reader.sourcePos);
		assertEquals("helloworld".length(), reader.sourceLen);
		assertEquals("helloworld".length(), TrieParserReader.parseHasContentLength(reader));

		assertEquals(value1, TrieParserReader.parseNext(reader, map));
		assertEquals(-1, TrieParserReader.parseNext(reader, map));


	}


	@Test
	public void testUTF8Set() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(1000);
		map.setUTF8Value("helloworld",value1);
		map.setUTF8Value("tuesday",   value2);
		map.setUTF8Value("hello","2", value3);
		map.setUTF8Value("X%b","web", value4);
		map.setUTF8Value("%b","web",  value5);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value1, TrieParserReader.query(reader,map, "helloworld".getBytes(), 0, 10, 15));
		assertEquals(value2, TrieParserReader.query(reader,map, "tuesday".getBytes(),    0,  7, 15));
		assertEquals(value3, TrieParserReader.query(reader,map, "hello2".getBytes(),     0,  6, 15));
		assertEquals(value4, TrieParserReader.query(reader,map, "Xtheweb".getBytes(),    0,  7, 15));
		assertEquals(value5, TrieParserReader.query(reader,map, "theweb".getBytes(),     0,  6, 15));

		String actual = TrieParserReader.capturedFieldBytesAsUTF8(reader,0, new StringBuilder()).toString();
		assertEquals("the",actual);

	}
	
	@Test
	public void testUTF8Set2() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(1000, false);
		map.setUTF8Value("topic19",         value1);
		map.setUTF8Value("/testTopic/%b",   value2);

		String message = "\n"+map.toString();
		assertFalse(message,map.toString().contains("ERROR"));
		
		assertEquals(value1, TrieParserReader.query(reader,map, "topic19".getBytes(),         0, 7, 15));
		assertEquals(value2, TrieParserReader.query(reader,map, "/testTopic/%b".getBytes(),   0,  13, 15));
		assertEquals(value2, TrieParserReader.query(reader,map, "/testTopic/goob".getBytes(), 0,  15, 15));

		String actual = TrieParserReader.capturedFieldBytesAsUTF8(reader,0, new StringBuilder()).toString();
		assertEquals("goob",actual);
		
	}
	
	
	@Test 
	public void testExtractBytesMiddle() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(data1, 0, 3, Integer.MAX_VALUE, value1);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(dataBytesExtractMiddle, 0, dataBytesExtractMiddle.length, Integer.MAX_VALUE, value2);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 2, 3, Integer.MAX_VALUE, value3);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 3, Integer.MAX_VALUE));
		assertEquals(value3, TrieParserReader.query(reader,map,data1, 2, 3, Integer.MAX_VALUE));


		assertEquals(value2, TrieParserReader.query(reader,map,toParseMiddle, 0, toParseMiddle.length, Integer.MAX_VALUE));

		assertEquals(1, TrieParserReader.capturedFieldCount(reader));

		byte[] readInto = new byte[]{0,0,0,0};        
		TrieParserReader.capturedFieldBytes(reader, 0, readInto, 0, 7);
		byte[] expected = new byte[]{10,11,12,13};
		assertEquals(Arrays.toString(expected),Arrays.toString(readInto) );

		int j = 4;
		while (--j>=0) {
			int b = TrieParserReader.capturedFieldByte(reader, 0, j);
			assertEquals(expected[j],b);
		}        
	}

	@Test 
	public void testExtractBytesBeginning() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(wrapping(data1,3), 0, 3, 7, value1);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractBeginning,3), 0, dataBytesExtractBeginning.length, 7, value2);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(data1,3), 2, 3, 7, value3);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value1, TrieParserReader.query(reader,map,wrapping(data1,3), 0, 3, 7));
		assertEquals(value3, TrieParserReader.query(reader,map,wrapping(data1,3), 2, 3, 7));

		assertEquals(value2, TrieParserReader.query(reader,map,wrapping(toParseBeginning,3), 0, toParseBeginning.length, 7));

		assertEquals(1, TrieParserReader.capturedFieldCount(reader));

		byte[] target = new byte[100];
		int len = TrieParserReader.capturedFieldBytes(reader, 0, target, 0, 63);
		assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(Arrays.copyOfRange(target,0,len)) );

	}


	@Test
	public void testSimpleMultipleParse() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);

		byte[] a = "StringA".getBytes();
		byte[] b = "BytesB".getBytes();

		map.setValue(a, 0, a.length, 31, 1);
		map.setValue(b, 0, b.length, 31, 8);

		byte[] testBytes = "BytesBStringA".getBytes();

		TrieParserReader.parseSetup(reader, testBytes, 0, testBytes.length, 31);

		long valueB = TrieParserReader.parseNext(reader, map);
		assertEquals(8L, valueB);

		long valueA = TrieParserReader.parseNext(reader, map);
		assertEquals(1L, valueA);        

	}

	@Test
	public void testExtractMultipleParse() {

		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		byte[] a = "StringA%b ".getBytes();
		byte[] b = "BytesB%b ".getBytes();

		map.setValue(a, 0, a.length, 31, 1);
		map.setValue(b, 0, b.length, 31, 8);

		byte[] testBytes = "BytesBCAPTURE StringACAPTURE ".getBytes();

		TrieParserReader.parseSetup(reader, testBytes, 0, testBytes.length, 31);

		long valueB = TrieParserReader.parseNext(reader, map);
		assertEquals(8, valueB);
		assertEquals(1,TrieParserReader.capturedFieldCount(reader));

		byte[] expected = new byte[]{0,0,0,0,0,0,0};        
		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{'C','A','P','T','U','R','E'}), Arrays.toString(expected) );

		long valueA = TrieParserReader.parseNext(reader, map);
		assertEquals(1, valueA);        
		assertEquals(1,TrieParserReader.capturedFieldCount(reader));

		expected = new byte[]{0,0,0,0,0,0,0};        
		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{'C','A','P','T','U','R','E'}), Arrays.toString(expected) );
	}


	@Test
	public void testSimpleValueReplace() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);

		map.setValue(data1, 0, 3, 7, value1);        
		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 3, 7));

		map.setValue(data1, 0, 3, 7, value2);        
		assertEquals(value2, TrieParserReader.query(reader,map,data1, 0, 3, 7));

	}

	@Test
	public void testSimpleValueReplaceWrapping() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);


		map.setValue(data1, 5, 5, 7, value1);        
		assertEquals(value1, TrieParserReader.query(reader,map,data1, 5, 5, 7));

		map.setValue(data1, 5, 5, 7, value2);        
		assertEquals(value2, TrieParserReader.query(reader,map,data1, 5, 5, 7));

	}

	@Test
	public void testTwoNonOverlapValuesWithReplace() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data1, 1, 3, 7, value1);
		map.setValue(data2, 1, 3, 7, value2);

		assertEquals(value1, TrieParserReader.query(reader,map,data1, 1, 3, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data2, 1, 3, 7));

		//swap values
		map.setValue(data1, 1, 3, 7, value2);
		map.setValue(data2, 1, 3, 7, value1);

		assertEquals(value2, TrieParserReader.query(reader,map,data1, 1, 3, 7));
		assertEquals(value1, TrieParserReader.query(reader,map,data2, 1, 3, 7));        

	}

	@Test
	public void testTwoNonOverlapValuesWrappingWithReplace() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data1, 5, 5, 7, value1);
		map.setValue(data2, 5, 5, 7, value2);

		assertEquals(value1, TrieParserReader.query(reader,map,data1, 5, 5, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data2, 5, 5, 7));

		//swap values
		map.setValue(data1, 5, 5, 7, value2);
		map.setValue(data2, 5, 5, 7, value1);

		assertEquals(value2, TrieParserReader.query(reader,map,data1, 5, 5, 7));
		assertEquals(value1, TrieParserReader.query(reader,map,data2, 5, 5, 7));        
	}


	@Test
	public void testTwoOverlapValues() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data2, 2, 5, 7, value1);
		map.setValue(data3, 2, 5, 7, value2);

		assertEquals(value1, TrieParserReader.query(reader,map,data2, 2, 5, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data3, 2, 5, 7));

		//swap values
		map.setValue(data2, 2, 5, 7, value2);
		map.setValue(data3, 2, 5, 7, value1);

		assertEquals(value2, TrieParserReader.query(reader,map,data2, 2, 5, 7));
		assertEquals(value1, TrieParserReader.query(reader,map,data3, 2, 5, 7));        

	}

	@Test
	public void testThreeOverlapValues() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data3, 2, 5, 7, value2);
		map.setValue(data4, 2, 5, 7, value3);
		map.setValue(data2, 2, 5, 7, value1);

		assertEquals(value1, TrieParserReader.query(reader,map,data2, 2, 5, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data3, 2, 5, 7));
		assertEquals(value3, TrieParserReader.query(reader,map,data4, 2, 5, 7));

		//swap values
		map.setValue(data2, 2, 5, 7, value3);
		map.setValue(data3, 2, 5, 7, value2);
		map.setValue(data4, 2, 5, 7, value1);

		assertEquals(value1, TrieParserReader.query(reader,map,data4, 2, 5, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data3, 2, 5, 7));
		assertEquals(value3, TrieParserReader.query(reader,map,data2, 2, 5, 7));        

	}

	@Test
	public void testInsertBeforeBranch() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data3, 0, 6, 7, value1);
		map.setValue(data4, 0, 6, 7, value2);
		map.setValue(data5, 0, 6, 7, value3);


		assertEquals(value1, TrieParserReader.query(reader,map,data3, 0, 6, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data4, 0, 6, 7));
		assertEquals(value3, TrieParserReader.query(reader,map,data5, 0, 6, 7));

		//swap values
		map.setValue(data3, 0, 6, 7, value3);
		map.setValue(data4, 0, 6, 7, value2);
		map.setValue(data5, 0, 6, 7, value1);

		assertEquals(value1, TrieParserReader.query(reader,map,data5, 0, 6, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data4, 0, 6, 7));
		assertEquals(value3, TrieParserReader.query(reader,map,data3, 0, 6, 7));        

	}

	@Test
	public void testInsertAfterBothBranchs() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data2,  1, 7, 7, value1);
		map.setValue(data3,  1, 7, 7, value2);
		map.setValue(data2b, 1, 7, 7, value3);
		map.setValue(data3b, 1, 7, 7, value4);

		assertEquals(value1, TrieParserReader.query(reader,map,data2,  1, 7, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data3,  1, 7, 7));
		assertEquals(value3, TrieParserReader.query(reader,map,data2b, 1, 7, 7));
		assertEquals(value4, TrieParserReader.query(reader,map,data3b, 1, 7, 7));

		//swap values
		map.setValue(data3b, 1, 7, 7, value1);
		map.setValue(data2b, 1, 7, 7, value2);
		map.setValue(data3,  1, 7, 7, value3);
		map.setValue(data2,  1, 7, 7, value4);

		assertEquals(value4, TrieParserReader.query(reader,map,data2,  1, 7, 7));
		assertEquals(value3, TrieParserReader.query(reader,map,data3,  1, 7, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data2b, 1, 7, 7));
		assertEquals(value1, TrieParserReader.query(reader,map,data3b, 1, 7, 7));       

	}


	@Test
	public void testLongInsertThenShortRootInsert() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(1000);        

		map.setValue(data1, 0, 8, 7, value1);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 0, 3, 7, value2);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 8, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data1, 0, 3, 7));

		//swap values
		map.setValue(data1, 0, 8, 7, value2);
		map.setValue(data1, 0, 3, 7, value1);

		assertEquals(value2, TrieParserReader.query(reader,map,data1, 0, 8, 7));
		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 3, 7));        

	}


	@Test
	public void testShortRootInsertThenLongInsert() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data1, 0, 3, 7, value2);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 0, 8, 7, value1);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 8, 7));
		assertEquals(value2, TrieParserReader.query(reader,map,data1, 0, 3, 7));

		//swap values
		map.setValue(data1, 0, 3, 7, value1);
		map.setValue(data1, 0, 8, 7, value2);

		assertEquals(value2, TrieParserReader.query(reader,map,data1, 0, 8, 7));
		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 3, 7));        

	}

	//add tests for end stopping at the branch point?  double check the coverage

	@Test
	public void testByteExtractExample() {

		TrieParserReader reader = new TrieParserReader(10);
		TrieParser map = new TrieParser(16);  

		byte[] b1 = "X-Wap-Profile:%b\r\n".getBytes();

		//  byte[] b2= "X-Online-Host:%b\r\n".getBytes();
		byte[] b2 = "Content-Length: %u\r\n".getBytes();

		byte[] b3 = "X-ATT-DeviceId:%b\r\n".getBytes();
		byte[] b4 = "X-ATT-DeviceId:%b\n".getBytes(); //testing same text with different ending

		byte[] b5 = "\r\n".getBytes(); //testing detection of empty line without capture.
		byte[] b6 = "%b\r\n".getBytes(); //testing capture of unknown pattern from the beginning

		int bits = 7;
		int mask = (1<<bits)-1;

		map.setValue(wrapping(b1,bits), 0, b1.length, mask, 1);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b2,bits), 0, b2.length, mask, 2);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b3,bits), 0, b3.length, mask, 3);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b4,bits), 0, b4.length, mask, 4); 
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b5,bits), 0, b5.length, mask, 5);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b6,bits), 0, b6.length, mask, 6);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		byte[] example = "X-Wap-Profile:ABCD\r\nHello".getBytes();
		assertEquals(1, TrieParserReader.query(reader,  map, wrapping(example,bits), 0, example.length, mask));

		byte[] expected = new byte[]{0,0,0,0};        
		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{'A','B','C','D'}),Arrays.toString(expected) );

		byte[] example1 = "Content-Length: 1234\r\n".getBytes();
		assertEquals(2, TrieParserReader.query(reader, map, wrapping(example1,bits), 0, example1.length, mask));

		int[] target = new int[]{0,0,0,0};
		TrieParserReader.capturedFieldInts(reader, 0, target, 0);
		assertEquals(1,target[0]); //positive

		byte[] example6 = "%b\r\n".getBytes();  // wildcard of wildcard
		assertEquals(6, TrieParserReader.query(reader, map, wrapping(example6,bits), 0, example6.length, mask));

		byte[] example3 = "X-ATT-DeviceId:%b\r\n".getBytes(); // wildcard of wildcard
		assertEquals(3, TrieParserReader.query(reader, map, wrapping(example3,bits), 0, example3.length, mask));

		byte[] example2 = "Content-Length: %u\r\n".getBytes();// wildcard of wildcard
		assertEquals(2, TrieParserReader.query(reader, map, wrapping(example2,bits), 0, example2.length, mask));



		//////////////////////////////////////TESTING DUMP
		//System.out.println(map);
		//THESE TWO PROBLEMS ARE THE PRIMARY CAAUSE OF OUR SLOWDOWNS.
		//An ALT_BRANCH happens at the front due to immmediage %d capture
		//An ALT_BRANCH also happens due to both \n and \r\n endings 
		////////////

		assertEquals(0,target[1]);//no high int        
		assertEquals(1234,target[2]);
		short base = (short)(target[3]>>16);
		short digits = (short)(target[3]);
		assertEquals(10, base);
		assertEquals(4, digits);        

	}


	private byte[] wrapping(byte[] data, int bits) {
		int len = 1<<bits;
		byte[] result = new byte[len];
		System.arraycopy(data, 0, result, 0, data.length);
		return result;
	}

	private byte[] wrappingRolledOver(byte[] data, int bits, int start) {
		int len = 1<<bits;
		int msk = len-1;
		byte[] result = new byte[len];
		
		int i = data.length;
		while (--i>=0) {
			result[(start+i) & msk] = data[i];
		}
		
		return result;
	}
	

	@Test
	public void testToString() {

		TrieParser map = new TrieParser(16);        

		map.setValue(wrapping(data1,3), 0, 3, 7, value2);
		map.setValue(wrapping(data1,3), 0, 8, 7, value1);

		map.setValue(wrapping(data2,3),  1, 7, 7, value1);
		map.setValue(wrapping(data3,3),  1, 7, 7, value2);
		map.setValue(wrapping(data2b,3), 1, 7, 7, value3);
		map.setValue(wrapping(data3b,3), 1, 7, 7, value4);

		String actual = map.toString();

		String expected = "BRANCH_VALUE1[0], -248[1], 0[2], 49[3], \n"+
				"RUN0[4], 3[5], 107'k'[6], 108'l'[7], 109'm'[8], \n"+
				"BRANCH_VALUE1[9], -240[10], 0[11], 20[12], \n"+
				"RUN0[13], 2[14], 120'x'[15], 121'y'[16], \n"+
				"BRANCH_VALUE1[17], -128[18], 0[19], 6[20], \n"+
				"RUN0[21], 2[22], -128[23], -127[24], \n"+
				"END7[25], 47[26], \n"+
				"RUN0[27], 2[28], 122'z'[29], 123'{'[30], \n"+
				"END7[31], 23[32], \n"+
				"RUN0[33], 2[34], 110'n'[35], 111'o'[36], \n"+
				"BRANCH_VALUE1[37], -252[38], 0[39], 6[40], \n"+
				"RUN0[41], 2[42], 118'v'[43], 119'w'[44], \n"+
				"END7[45], 35[46], \n"+
				"RUN0[47], 2[48], 112'p'[49], 113'q'[50], \n"+
				"END7[51], 10[52], \n"+
				"RUN0[53], 3[54], 101'e'[55], 102'f'[56], 103'g'[57], \n"+
				"SAFE6[58], 23[59], \n"+
				"RUN0[60], 5[61], 104'h'[62], 105'i'[63], 106'j'[64], 107'k'[65], 108'l'[66], \n"+
				"END7[67], 10[68], \n";

		if (!expected.equals(actual)) {
			System.out.println("String expected = \""+(actual.replace("\n", "\\n\"+\n\"")));
		}

		assertEquals(expected,actual);


		int actualLimit = map.getLimit();
		assertEquals(69, actualLimit);

	}

	@Test
	public void testToDot() {

		TrieParser map = new TrieParser(16);        

		map.setValue(wrapping(data1,3), 0, 3, 7,  value2);    // 101,102,103
		map.setValue(wrapping(data1,3), 0, 8, 7,  value1);    // 101,102,103,104,105,106,107,108    

		//System.out.println(map.toDOT(new StringBuilder()));

		map.setValue(wrapping(data2,3),  1, 7, 7, value1);    // 107,108,109,110,111,112,113
		map.setValue(wrapping(data3,3),  1, 7, 7, value2);    // 107,108,109,120,121,122,123

		//System.out.println(map.toDOT(new StringBuilder()));


		map.setValue(wrapping(data2b,3), 1, 7, 7, value3);    // 107,108,109,110,111,118,119
		map.setValue(wrapping(data3b,3), 1, 7, 7, value4);    // 107,108,109,120,121,(byte)128,(byte)129

		String actual = map.toDOT(new StringBuilder()).toString();

		String expected = "digraph {\n"+
				"node0[label=\"BRANCH ON BIT\n"+
				" bit:00001000\"]\n"+
				"node0->node4\n"+
				"node0->node53\n"+
				"node4[label=\"RUN of 3\n"+
				"klm\"]\n"+
				"node4->node9\n"+
				"node9[label=\"BRANCH ON BIT\n"+
				" bit:00010000\"]\n"+
				"node9->node13\n"+
				"node9->node33\n"+
				"node13[label=\"RUN of 2\n"+
				"xy\"]\n"+
				"node13->node17\n"+
				"node17[label=\"BRANCH ON BIT\n"+
				" bit:10000000\"]\n"+
				"node17->node21\n"+
				"node17->node27\n"+
				"node21[label=\"RUN of 2\n"+
				"{(-128)}{(-127)}\"]\n"+
				"node21->node25\n"+
				"node25[label=\"END47[26]\"]\n"+
				"node27[label=\"RUN of 2\n"+
				"z{\"]\n"+
				"node27->node31\n"+
				"node31[label=\"END23[32]\"]\n"+
				"node33[label=\"RUN of 2\n"+
				"no\"]\n"+
				"node33->node37\n"+
				"node37[label=\"BRANCH ON BIT\n"+
				" bit:00000100\"]\n"+
				"node37->node41\n"+
				"node37->node47\n"+
				"node41[label=\"RUN of 2\n"+
				"vw\"]\n"+
				"node41->node45\n"+
				"node45[label=\"END35[46]\"]\n"+
				"node47[label=\"RUN of 2\n"+
				"pq\"]\n"+
				"node47->node51\n"+
				"node51[label=\"END10[52]\"]\n"+
				"node53[label=\"RUN of 3\n"+
				"efg\"]\n"+
				"node53->node58\n"+
				"node58[label=\"SAFE23[59], \"]\n"+
				"node58->node60\n"+
				"node60[label=\"RUN of 5\n"+
				"hijkl\"]\n"+
				"node60->node67\n"+
				"node67[label=\"END10[68]\"]\n"+
				"}\n";


		if (!expected.equals(actual)) {
			System.out.println("String expected = \""+(actual.replace("\"", "\\\"").replace("\n", "\\n\"+\n\"")  ));
		}

		assertEquals(expected,actual);


		int actualLimit = map.getLimit();
		assertEquals(69, actualLimit);

	}


	@Test
	public void testDisabledEscapedEscape() {

		TrieParser map = new TrieParser(1000, 1, true, false);        
		TrieParserReader reader = new TrieParserReader();

		map.setValue(wrapping(data1,3), 0, 3, 7, value2);
		map.setValue(wrapping(data1,3), 0, 8, 7, value1);        
		map.setValue(wrapping(escapedEscape,3), 1, 7, 7, value3);

		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(data1,3), 0, 3, 7));
		assertEquals(value1, TrieParserReader.query(reader,map, wrapping(data1,3), 0, 8, 7));
		assertEquals(value3, TrieParserReader.query(reader,map, wrapping(escapedEscape,3), 1, 7, 7));

		map.setValue(wrapping(data2,3),  1, 7, 7, value1);
		map.setValue(wrapping(data3,3),  1, 7, 7, value2);

		assertEquals(value1, TrieParserReader.query(reader,map, wrapping(data2,3), 1, 7, 7));
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(data3,3), 1, 7, 7));

		map.setValue(wrapping(data2b,3), 1, 7, 7, value3);
		map.setValue(wrapping(data3b,3), 1, 7, 7, value4);
		map.setValue(wrapping(escapedEscape,3), 1, 7, 7, value2);

		assertEquals(value3, TrieParserReader.query(reader,map, wrapping(data2b,3), 1, 7, 7));
		assertEquals(value4, TrieParserReader.query(reader,map, wrapping(data3b,3), 1, 7, 7)); 
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(escapedEscape,3), 1, 7, 7));

	}

	@Test
	public void testURLExtract() {

		TrieParser trie = new TrieParser(1000, 1, true, true);        
		TrieParserReader reader = new TrieParserReader(3);


		trie.setUTF8Value("#",  1);  //Ignores
		trie.setUTF8Value(":",  1);  //Ignores
		trie.setUTF8Value(";",  1);  //Ignores
		trie.setUTF8Value(",",  1);  //Ignores
		trie.setUTF8Value("!",  1);  //Ignores
		trie.setUTF8Value("?",  1);  //Ignores
		trie.setUTF8Value("\\", 1);  //Ignores
		trie.setUTF8Value("/",  1);  //Ignores
		trie.setUTF8Value(" ",  1);  //Ignores
		trie.setUTF8Value("\"", 1);  //Ignores
		trie.setUTF8Value(" ",  1);  //Ignores
		trie.setUTF8Value("'",  1);  //Ignores
		trie.setUTF8Value("&",  1);  //Ignores
		trie.setUTF8Value("-",  1);  //Ignores
		trie.setUTF8Value("+",  1);  //Ignores
		trie.setUTF8Value("|",  1);  //Ignores
		trie.setUTF8Value(">",  1);  //Ignores
		trie.setUTF8Value("_",  1);  //Ignores
		trie.setUTF8Value("^",  1);  //Ignores
		trie.setUTF8Value(".",  1);  //Ignores
		trie.setUTF8Value(")",  1);  //Ignores
		trie.setUTF8Value("<",  1);  //Ignores
		trie.setUTF8Value("[",  1);  //Ignores
		trie.setUTF8Value("]",  1);  //Ignores
		trie.setUTF8Value("$",  1);  //Ignores
		trie.setUTF8Value("~",  1);  //Ignores

		trie.setUTF8Value("%b?",  2); //new word
		trie.setUTF8Value("%b\"", 2); //new word
		trie.setUTF8Value("%b ",  2); //new word
		trie.setUTF8Value("%b.",  2); //new word
		trie.setUTF8Value("%b,",  2); //new word
		trie.setUTF8Value("%b!",  2); //new word
		trie.setUTF8Value("%b:",  2); //new word //NOTE: this one is the second choice because http starts with literal chars.
		trie.setUTF8Value("%b(",  2); //new word
		trie.setUTF8Value("%b)",  2); //new word
		trie.setUTF8Value("%b+",  2); //new word
		trie.setUTF8Value("%b-",  2); //new word
		trie.setUTF8Value("%b_",  2); //new word
		trie.setUTF8Value("%b[",  2); //new word
		trie.setUTF8Value("%b]",  2); //new word
		trie.setUTF8Value("%b{",  2); //new word
		trie.setUTF8Value("%b}",  2); //new word 

		//the : and s cause a branch so we must check for s://
		trie.setUTF8Value("http://%b " ,3);//URL  //NOTE these are the first attempted to match due to their starting with litterals.
		trie.setUTF8Value("https://%b ",4);//URL


		assertFalse(trie.toString(),trie.toString().contains("ERROR"));


		byte[] source = "& http://google.com/stuff $https://another.com/g➕g❤️%s #Hello ".getBytes(); //space is required to mark end of text.

		TrieParserReader.parseSetup(reader, source, 0, source.length, 1023); 

		assertEquals(1,TrieParserReader.parseNext(reader, trie));
		assertEquals(1,TrieParserReader.parseNext(reader, trie));
		assertEquals(3,TrieParserReader.parseNext(reader, trie));

		try {
			URL url = new URL(TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, new StringBuilder("http://")).toString());
		} catch (MalformedURLException e) {
			fail(e.getMessage());
		}      

		assertEquals(1,TrieParserReader.parseNext(reader, trie));

		//this URL has UTF8 odd chars and should not matter, we should still be able to parse and extract the value.
		assertEquals(4,TrieParserReader.parseNext(reader, trie));
		try {
			URL url = new URL(TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, new StringBuilder("https://")).toString());
		} catch (MalformedURLException e) {
			fail(e.getMessage());
		}     

		assertEquals(1,TrieParserReader.parseNext(reader, trie));         
		assertEquals(2,TrieParserReader.parseNext(reader, trie));

		assertFalse(TrieParserReader.parseHasContent(reader));

		//byte[] realWorldSource = "https… ".getBytes();
		byte[] realWorldSource ="RT @CITmagazine: From #CITAList today is Katherine Bell, CWT Meetings & Events: https://t.co/UYkOLYKkBE  #eventprofs @CWT_UKI @CWT_ME https… ".getBytes();
		//  byte[] realWorldSource = "Antisocial Social Worker tweeting Freudian scripts...... Favs:https://t.co/6OWZw8D6CV Recents: https://t.co/zC4BYUhsR0 #EnvyDaStrength ".getBytes();

		//System.out.println("TRIE: \n"+trie);

		TrieParserReader.parseSetup(reader, wrapping(realWorldSource,10), 0, realWorldSource.length, 1023);
		while (TrieParserReader.parseHasContent(reader)) {
			int token = (int) TrieParserReader.parseNext(reader, trie);

			if (-1==token) {
				byte[] copyOfRange = Arrays.copyOfRange(realWorldSource, reader.sourcePos, realWorldSource.length);
				String value = new String(  copyOfRange );
				System.out.println("data '"+value+"'"+"  bytes "+Arrays.toString(copyOfRange));

			}

			assertFalse(reader.sourceLen+" at "+reader.sourcePos+" len "+realWorldSource.length, -1 == token);
		}


	}

	@Test
	public void testCustomEscapeChar() {

		TrieParser parser = new TrieParser(1000, 1, true, true, false, (byte)'"');  
		TrieParserReader reader = new TrieParserReader(4, true);

		parser.setUTF8Value("#{\"b}", TrieParser.ESCAPE_CMD_SIGNED_INT); //%i
		parser.setUTF8Value("#\"b/", TrieParser.ESCAPE_CMD_SIGNED_INT);  //%i
		parser.setUTF8Value("#\"b?", TrieParser.ESCAPE_CMD_SIGNED_INT);  //%i
		parser.setUTF8Value("#\"b&", TrieParser.ESCAPE_CMD_SIGNED_INT);  //%i
		parser.setUTF8Value("#\"b", TrieParser.ESCAPE_CMD_SIGNED_INT);   //%i

		parser.setUTF8Value("^{\"b}", TrieParser.ESCAPE_CMD_DECIMAL);  //%i%.
		parser.setUTF8Value("^\"b/", TrieParser.ESCAPE_CMD_DECIMAL);   //%i%.
		parser.setUTF8Value("^\"b?", TrieParser.ESCAPE_CMD_DECIMAL);   //%i%.
		parser.setUTF8Value("^\"b&", TrieParser.ESCAPE_CMD_DECIMAL);   //%i%.
		parser.setUTF8Value("^\"b", TrieParser.ESCAPE_CMD_DECIMAL);    //%i%.

		parser.setUTF8Value("${\"b}", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$\"b?", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$\"b", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$\"b&", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$\"b/", TrieParser.ESCAPE_CMD_BYTES);

		parser.setUTF8Value("%{\"b}", TrieParser.ESCAPE_CMD_RATIONAL); //%i%/
		parser.setUTF8Value("%\"b/", TrieParser.ESCAPE_CMD_RATIONAL);  //%i%/
		parser.setUTF8Value("%\"b?", TrieParser.ESCAPE_CMD_RATIONAL);  //%i%?
		parser.setUTF8Value("%\"b&", TrieParser.ESCAPE_CMD_RATIONAL);  //%i%&
		parser.setUTF8Value("%\"b", TrieParser.ESCAPE_CMD_RATIONAL);   //%i%/

		//	parser.toDOT(System.out);

		findShortText(parser, reader, "$hello?", TrieParser.ESCAPE_CMD_BYTES);
		findShortText(parser, reader, "$hello/", TrieParser.ESCAPE_CMD_BYTES);
		findShortText(parser, reader, "${hello}", TrieParser.ESCAPE_CMD_BYTES);
		findShortText(parser, reader, "$hello&", TrieParser.ESCAPE_CMD_BYTES);
		findShortText(parser, reader, "$hello", TrieParser.ESCAPE_CMD_BYTES);


		findShortText(parser, reader, "#hello?", TrieParser.ESCAPE_CMD_SIGNED_INT);
		findShortText(parser, reader, "#hello/", TrieParser.ESCAPE_CMD_SIGNED_INT);
		findShortText(parser, reader, "#{hello}", TrieParser.ESCAPE_CMD_SIGNED_INT);
		findShortText(parser, reader, "#hello&", TrieParser.ESCAPE_CMD_SIGNED_INT);
		findShortText(parser, reader, "#hello", TrieParser.ESCAPE_CMD_SIGNED_INT);


		findShortText(parser, reader, "^hello?", TrieParser.ESCAPE_CMD_DECIMAL);
		findShortText(parser, reader, "^hello/", TrieParser.ESCAPE_CMD_DECIMAL);
		findShortText(parser, reader, "^{hello}", TrieParser.ESCAPE_CMD_DECIMAL);
		findShortText(parser, reader, "^hello&", TrieParser.ESCAPE_CMD_DECIMAL);
		findShortText(parser, reader, "^hello", TrieParser.ESCAPE_CMD_DECIMAL);

		findShortText(parser, reader, "%hello?", TrieParser.ESCAPE_CMD_RATIONAL);
		findShortText(parser, reader, "%hello/", TrieParser.ESCAPE_CMD_RATIONAL);
		findShortText(parser, reader, "%{hello}", TrieParser.ESCAPE_CMD_RATIONAL);
		findShortText(parser, reader, "%hello&", TrieParser.ESCAPE_CMD_RATIONAL);
		findShortText(parser, reader, "%hello", TrieParser.ESCAPE_CMD_RATIONAL);

	}

	@Test
	public void testPatternExtraction() {
		TrieParser parser = new TrieParser(1000, 1, true, true);  
		TrieParserReader reader = new TrieParserReader(4, true);


		parser.setUTF8Value("$%b/", TrieParser.ESCAPE_CMD_BYTES);
		assertFalse(parser.toString(),parser.toString().contains("ERROR"));

		parser.setUTF8Value("${%b}", TrieParser.ESCAPE_CMD_BYTES);
		assertFalse(parser.toString(),parser.toString().contains("ERROR"));

		parser.setUTF8Value("$%b?", TrieParser.ESCAPE_CMD_BYTES);
		assertFalse(parser.toString(),parser.toString().contains("ERROR"));

		parser.setUTF8Value("$%b", TrieParser.ESCAPE_CMD_BYTES);
		assertFalse(parser.toString(),parser.toString().contains("ERROR"));

		parser.setUTF8Value("$%b&", TrieParser.ESCAPE_CMD_BYTES);
		assertFalse(parser.toString(),parser.toString().contains("ERROR"));

		parser.toDOT(System.out);

		//for every non match just consume the char and move to the next		

		findShortText(parser, reader, "$hello?", TrieParser.ESCAPE_CMD_BYTES);
		findShortText(parser, reader, "$hello/", TrieParser.ESCAPE_CMD_BYTES);
		findShortText(parser, reader, "${hello}", TrieParser.ESCAPE_CMD_BYTES);
		findShortText(parser, reader, "$hello&", TrieParser.ESCAPE_CMD_BYTES);
		findShortText(parser, reader, "$hello", TrieParser.ESCAPE_CMD_BYTES);	
	}

	private void findShortText(TrieParser parser, TrieParserReader reader, String text, int match) {
		byte[] bytes = wrapping(text.getBytes(),4);
		assertEquals(match, reader.query(reader, parser, bytes, 0, text.length(), 15));
	}

	@Test
	public void testEscapedEscape() {

		TrieParser map = new TrieParser(1000, 1, true, true);        
		TrieParserReader reader = new TrieParserReader();

		map.setValue(wrapping(data1,3), 0, 3, 7, value2);
		map.setValue(wrapping(data1,3), 0, 8, 7, value1);        
		map.setValue(wrapping(escapedEscape,3), 1, 7, 7, value3);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(data1,3), 0, 3, 7));
		assertEquals(value1, TrieParserReader.query(reader,map, wrapping(data1,3), 0, 8, 7));
		//assertEquals(value3, TrieParserReader.query(reader,map, wrapping(escapedEscape,3), 1, 7, 7));

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(data2,3),  1, 7, 7, value1);
		map.setValue(wrapping(data3,3),  1, 7, 7, value2);

		assertEquals(value1, TrieParserReader.query(reader,map, wrapping(data2,3), 1, 7, 7));
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(data3,3), 1, 7, 7));

		map.setValue(wrapping(data2b,3), 1, 7, 7, value3);
		map.setValue(wrapping(data3b,3), 1, 7, 7, value4);
		map.setValue(wrapping(escapedEscape,3), 1, 7, 7, value2);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value3, TrieParserReader.query(reader,map, wrapping(data2b,3), 1, 7, 7));
		assertEquals(value4, TrieParserReader.query(reader,map, wrapping(data3b,3), 1, 7, 7)); 
		assertEquals(value2, TrieParserReader.query(reader,map, wrapping(escapedEscape,3), 1, 7, 7));

	}

	@Test 
	public void testExtractBytesEnd_temp() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(data1, 0, 3, 7, value1); //101,102,103
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd,3), 0, dataBytesExtractEnd.length, 7, value2);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 2, 3, 7, value3); //103,104,105
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value1, TrieParserReader.query(reader,map,data1, 0, 3, 7)); // 101,102,103

		assertEquals(value3, TrieParserReader.query(reader,map,data1, 2, 3, 7)); // 103,104,105

		assertEquals(value2, TrieParserReader.query(reader,map,toParseEnd, 0, toParseEnd.length, 7)); //100,101,102,10,11,12,13,127

		assertEquals(1, TrieParserReader.capturedFieldCount(reader));

		byte[] expected = new byte[]{0,0,0,0};        
		TrieParserReader.capturedFieldBytes(reader, 0, expected, 0, 7);
		assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(expected));
	}

	/*****************************
	 * 
	 * Test cases for Visitor
	 * 
	 * ***************************
	 */

	//Visitor for recording the results of each visit test case
	private final ByteTestSequenceVisitor visitor = new ByteTestSequenceVisitor();

	@Test 
	public void testVisitor() {
		TrieParserReader reader = new TrieParserReader(3,true);
		TrieParser map = new TrieParser(16,false);

		map.setValue(data1, 0, 3, 7, value2);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 0, 8, 7, value1); 
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data2,  1, 7, 7, value1);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data3,  1, 7, 7, value2); 
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data2b, 1, 7, 7, value3); 
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data3b, 1, 7, 7, value4);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

//VALUE1 = 10           //offset 2 len 3
		reader.visit(map, visitor, data1, 2, 3, 7);//103,104,105
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		System.out.println("vistor.toString(): " + visitor.toString());
		assertEquals("", visitor.toString()); 
	}

	/*
	 * This is the assumed example with map containing "catalog", "cat%b" and search string being "catalog"
	 */
	@Test 
	public void visitor_catalog_example() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(data_catalog, 0, data_catalog.length, 7, value8);
		assertFalse(map.toString(),map.toString().contains("ERROR"));


		map.setValue(data_cat_p_b, 0, data_cat_p_b.length, 7, value9);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		//System.out.println(map.toString());
		//System.out.println(map.toDOT(new StringBuilder()).toString());


		reader.visit(map, visitor,data_catalog, 0, data_catalog.length, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("91 93", visitor.toString());
	}

	@Test 
	public void testVisitorExtractMultiBytes() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(toParseEnd, 0, toParseEnd.length, 15, value4);
		map.setValue(toParseMiddle, 0, toParseMiddle.length, 15, value4);
		map.setValue(toParseBeginning, 0, toParseBeginning.length, 15, value4);        
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(dataBytesMultiBytes1, 0, 6, 7, value1);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value1, TrieParserReader.query(reader,map,dataBytesMultiBytesValue1, 0, dataBytesMultiBytesValue1.length, 15));

		map.setValue(dataBytesMultiBytes2, 0, dataBytesMultiBytes2.length, 15, value2);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		assertEquals(value2, TrieParserReader.query(reader,map,dataBytesMultiBytesValue2, 0, dataBytesMultiBytesValue2.length, 15));

		map.setValue(dataBytesMultiBytes3, 0, 5, 7, value3); //the /n is added last it takes priority and gets selected below.
		assertFalse(map.toString(),map.toString().contains("ERROR"));


		reader.visit(map, visitor,dataBytesMultiBytesValue1, 0, dataBytesMultiBytesValue1.length, 15);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35 10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,dataBytesMultiBytesValue2, 0, dataBytesMultiBytesValue2.length, 15);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,dataBytesMultiBytesValue3, 0, dataBytesMultiBytesValue3.length, 15);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();
	}

	@Test 
	public void testVisitorExtractBytesEnd() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(data1, 0, 3, 7, value1); //101,102,103
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd,3), 0, dataBytesExtractEnd.length, 7, value2);
		assertFalse(map.toString(),map.toString().contains("ERROR")); 

		map.setValue(data1, 2, 3, 7, value3); //103,104,105
		assertFalse(map.toString(),map.toString().contains("ERROR")); 


		reader.visit(map, visitor,data1, 0, 3, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data1, 2, 3, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();

		//error: Jump index exceeded  //Fixed 
		reader.visit(map, visitor,toParseEnd, 0, toParseEnd.length, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();
	}

	@Test 
	public void testVisitorExtractBytesEnd2b() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(wrapping(data1,4), 0, 3, 15, value1);                                          //1  added  101,102,103
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd,4), 0, dataBytesExtractEnd.length, 15, value2);   //2  added  100,101,102,'%','b',127
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd2,4), 0, dataBytesExtractEnd2.length, 15, value4); //4  added  100,101,102,'%','b',127,102
		assertFalse("\n"+map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 2, 3, 7, value3);                                                       //3  added  103,104,105


		reader.visit(map, visitor,wrapping(data1,4), 0, 3, 15);//101,102,103
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,wrapping(data1,4), 2, 3, 15);//103,104,105
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();

		//error: Jump index exceeded //Fixed
		
		//{100,101,102,'%','b',127,102} -> 47
		//{100,101,102,10,11,12,13,127,102};
		reader.visit(map, visitor,wrapping(toParseMiddleCopy,4), 0, toParseMiddleCopy.length, 15);//10,11,12,13,127,102
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("47", visitor.toString());
		visitor.clearResult();

		
		reader.visit(map, visitor,wrapping(toParseEndCopy,4), 2, toParseEndCopy.length, 15);//102,10,11,12,13,127
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("47", visitor.toString());
		visitor.clearResult();
	}

	@Test 
	public void testVisitorExtractBytesEndAll() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16,false);

		map.setValue(wrapping(data1,4), 0, 3, 15, value1);                                          //added  101,102,103
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEndA,4), 0, dataBytesExtractEndA.length, 15, value5); //added  100,101,102,'A','b',127 
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd,4), 0, dataBytesExtractEnd.length, 15, value2);   //added  100,101,102,'%','b',127
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEndB,4), 0, dataBytesExtractEndB.length, 15, value6); //added  100,101,102,'B','b',127 
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd2,4), 0, dataBytesExtractEnd2.length, 15, value4); //added  100,101,102,'%','b',127,102
		assertFalse("\n"+map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEndC,4), 0, dataBytesExtractEndC.length, 15, value7); //added   100,101,102,'C','b',127 
		assertFalse("\n"+map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 2, 3, 7, value3);  //    101,102,[ 103,104,105, ] 106,107,108
		assertFalse("\n"+map.toString(),map.toString().contains("ERROR"));


		reader.visit(map, visitor,wrapping(data1,4), 0, 3, 15);//101,102,103. 
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,wrapping(data1,4), 2, 3, 15);//103,104,105
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();

		
	
		// toParseMiddle    = new byte[]{100,101,10,11,12,13,127,102};
		
	
		//  dataBytesExtractEnd2 = new byte[]{100,101,102,'%','b',127,102}; -> mapped to 47    **%b is wildcard**
	
		//error: jump index exceeded //Fixed
		reader.visit(map, visitor,wrapping(toParseMiddleCopy,4), 0, toParseMiddleCopy.length, 15);// {100,101,102,10,11,12,13,127,102};
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		
		assertEquals("47", visitor.toString()); //-1 for sequential case & yielding "47" for visitor
		visitor.clearResult(); // value4

		
	//  dataBytesExtractEnd2 = new byte[]{100,101,102,'%','b',127,102}; -> mapped to 47
		
		
		//2 byte offset on test below. put 2 random values at beginning of 'copy' array to account for it.
		reader.visit(map, visitor,wrapping(toParseEndCopy,4), 2, toParseEndCopy.length, 15);// {98,99, 100,101,102,10,11,12,13,127,102}
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR")); 
		
		assertEquals("47", visitor.toString()); //23 for sequential case
		visitor.clearResult();

		//am I misunderstanding this? is it supposed to be taking teh last byte off the array or something?
		
		
	//  {100,101,102,'A','b',127}; -> mapped to 51
		//new byte[]{100,101,102,'%','b',127,102}; -> mapped to 47
		// {100,101,102,'%','b',127} -> mapped to 23
		reader.visit(map, visitor,wrapping(dataBytesExtractEndA,4), 0, dataBytesExtractEndA.length, 15);//100,101,102,'A','b',127
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		
		assertEquals("51 23", visitor.toString()); //51
		visitor.clearResult();

		//{100,101,102,'B','b',127}; -> mapped to 69
		
		reader.visit(map, visitor,wrapping(dataBytesExtractEndB,4), 0, dataBytesExtractEndB.length, 15);//100,101,102,'B','b',127
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
	
		assertEquals("69 23", visitor.toString()); //69
		visitor.clearResult();
			//{100,101,102,'C','b',127} -> 72
		// {100,101,102,'%','b',127} -> mapped to 23
		reader.visit(map, visitor,wrapping(dataBytesExtractEndC,4), 0, dataBytesExtractEndC.length, 15);//100,101,102,'C','b',127
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23 72", visitor.toString()); //72
		visitor.clearResult();
	}

	@Test 
	public void testVisitorExtractBytesEndStart() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(1000,false);

		map.setValue(wrapping(data1,4), 0, 3, 15, value1);                                              //101,102,103 e,f,g                                   
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractStart,4), 0, dataBytesExtractStart.length, 15, value2);  //{'%','b',127,100,101,102}; //def
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractStart2,4), 0, dataBytesExtractStart2.length, 15, value4);//{'%','b',127,102,101,102}; //fef
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractStart3,4), 0, dataBytesExtractStart3.length, 15, value1);//{'%','b',125,100,101,102};
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractStartA,4), 0, dataBytesExtractStartA.length, 15, value5); //'A','b',127,100,101,102 
		map.setValue(wrapping(dataBytesExtractStartB,4), 0, dataBytesExtractStartB.length, 15, value6);  


		assertFalse("\n"+map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractStartC,4), 0, dataBytesExtractStartC.length, 15, value7);  

		map.setValue(data1, 2, 3, 7, value3); //103,104,105


		reader.visit(map, visitor,wrapping(data1,4), 0, 3, 15);//101,102,103
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,wrapping(data1,4), 2, 3, 15);//103,104,105
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,wrapping(dataBytesExtractStartA,4), 0, dataBytesExtractStartA.length, 15);//'A','b',127,100,101,102
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("51 23", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,wrapping(dataBytesExtractStartB,4), 0, dataBytesExtractStartB.length, 15);//'B','b',127,100,101,102
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("69 23", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,wrapping(dataBytesExtractStartC,4), 0, dataBytesExtractStartC.length, 15);//'C','b',127,100,101,102
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23 72", visitor.toString());
		visitor.clearResult();

		
		//dataBytesExtractStartB = {'B','b',127,100,101,102} -> 69
		//{'%','b',127,100,101,102} -> 23
		reader.visit(map, visitor,wrapping(toParseStart,4), 0, toParseStart.length, 15);//10,20,30,127,100,101,102,111
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();
		
//{[101,102,103],104,105,106,107,108}; 101-103 -> 10
		reader.visit(map, visitor,wrapping(toParseStartx,4), 0, toParseStartx.length, 15);//10,20,30,125,100,111,111
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("", visitor.toString());
		visitor.clearResult();
	}


	@Test 
	public void testVisitorExtractBytesEndMulti() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(1000);

		map.setValue(data1, 0, 3, 7, value1); //101,102,103
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractEnd,3), 0, dataBytesExtractEnd.length, 7, value2);
		map.setValue(wrapping(dataBytesExtractEnd3,3), 0, dataBytesExtractEnd3.length, 7, value3);
		map.setValue(wrapping(dataBytesExtractEnd4,3), 0, dataBytesExtractEnd4.length, 7, value4);

		assertFalse(map.toString(),map.toString().contains("ERROR")); 

		map.setValue(data1, 2, 3, 7, value3); //103,104,105
		assertFalse(map.toString(),map.toString().contains("ERROR")); 


		reader.visit(map, visitor,data1, 0, 3, 7);//101,102,103
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data1, 2, 3, 7);//103,104,105
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,toParseEnd, 0, toParseEnd.length, 7);//100,101,102,10,11,12,13,127
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();
	}

	@Test
	public void testVisitorNonBranchInsert() {

		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		map.setUTF8Value("Hello: %u\r",   value2); //FYI, one should not see one of these in the wild often. 23
		map.setUTF8Value("Hello: %u\r\n", value3); //This just ends later so there is no branch .            35

		assertFalse(map.toString().contains("BRANCH_VALUE1"));

//this one fails, other does not. let me see
		byte[] text1 = "Hello: 123\r".getBytes();
		reader.visit(map, visitor,wrapping(text1,4), 0, text1.length, 15);
		System.out.println("visitor.toString() : " + visitor.toString());
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());//23 for sequential case  . should map to 23.
		visitor.clearResult();
System.out.println("**");
		byte[] text2 = "Hello: 123\r\n".getBytes();
		reader.visit(map, visitor,wrapping(text2,4), 0, text2.length, 15);
		System.out.println("visitor.toString() : " + visitor.toString());
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();
	}

	@Test
	public void testVisitorNumberAtEnd() {

		TrieParserReader reader = new TrieParserReader(2,true);
		TrieParser map = new TrieParser(16);
		map.setValue("unfollow/%u".getBytes(),0,"unfollow/%u".length(),Integer.MAX_VALUE, value2);

		byte[] pat = map.lastSetValueExtractonPattern();
		assertEquals(1,pat.length);
		assertEquals(TrieParser.ESCAPE_CMD_UNSIGNED_INT, pat[0]);

		assertFalse(map.toString(),map.toString().contains("ERROR"));


		byte[] text1 = "unfollow/123".getBytes();
		reader.visit(map, visitor,wrapping(text1,4), 0, text1.length, 15);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();

		byte[] text2 = "unfollow/%u".getBytes();
		reader.visit(map, visitor,wrapping(text2,4), 0, text2.length, 15);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();
	}

	@Test
	public void testVisitorSimpleURLPaths() {
		
		TrieParserReader reader = new TrieParserReader(3);
		
		TrieParser map = new TrieParser(16,false);
		map.setUTF8Value("/unfollow?user=%u",   value2); 
		map.setUTF8Value("/%b", value3); 
		//adding my own test to verify.
		map.setUTF8Value("/thisisatest", 50);
		
		//map.toDOT(System.out);// goes into console, save into txt run in console. save as dot file convert to graphviz

		assertFalse(map.toString(),map.toString().contains("ERROR"));
//
	
		byte[] text1 = "/unfollow?user=1234x".getBytes();

	
		reader.visit(map, visitor,wrapping(text1,5), 0, text1.length, 31);

		System.out.println("visitor.toString(): " + visitor.toString());
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));

		assertEquals("35 23", visitor.toString()); //23
		visitor.clearResult();
		
		
		
		byte[] text2 = "/Hello: 123\r\n".getBytes();

		reader.visit(map, visitor,wrapping(text2,5), 0, text2.length, 31);
	

		System.out.println("visitor.toString(): " + visitor.toString());
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString()); //35
	
		visitor.clearResult();
		
		

		byte[] text3 = "No root".getBytes();

		reader.visit(map, visitor,wrapping(text3,5), 0, text3.length, 31);
	
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));

		assertEquals("", visitor.toString()); //error here
		
		visitor.clearResult();
		
		
	
		byte[] text4 = "/thisisatest".getBytes();

		reader.visit(map, visitor,wrapping(text4,5), 0, text4.length, 31);
	

	
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		System.out.println("visitor.toString(): " + visitor.toString());
		assertEquals("50 35", visitor.toString()); 
	
		visitor.clearResult();
		
	}


	@Test
	public void testVisitorNumericPatternMatchesPatternDef() {

		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16,false);
		map.setUTF8Value("/unfollow?user=%u",   value2);
		map.setUTF8Value("/%b", value3); 

		assertFalse(map.toString(),map.toString().contains("ERROR"));


		byte[] text0 = "/unfollow?user=%u".getBytes();
		reader.visit(map, visitor,wrapping(text0,5), 0, text0.length, 31);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35 23", visitor.toString()); //23
		visitor.clearResult();
	}

	@Test
	public void testVisitorOrder1Insert() {

		//TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16, false);
		map.setUTF8Value("%bb\n",   value2); 
		String a = map.toString();

		map.setUTF8Value("ab\n",    value3);  
		String b = map.toString();
		assertFalse(a.equals(b));

		assertFalse(map.toString(),map.toString().contains("ERROR"));

	}

	@Test
	public void testVisitorOrder2Insert() {

		//TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);
		map.setUTF8Value("ab\n",    value3);  
		map.setUTF8Value("%bb\n",   value2); 
		map.setUTF8Value("bb\n",    value3);     

		assertFalse(map.toString(),map.toString().contains("ERROR"));
	}
	
//	@Test
//	public void testVisitorNestedPath() {
//
//		//TrieParserReader reader = new TrieParserReader(3);
//		TrieParser map = new TrieParser(16);  
//		//map.setUTF8Value("root/green/%b",    value3);     
//		//map.setUTF8Value("root/%b",         value2); 
//		
//		map.setUTF8Value("root/green/frequency",    value3);     
//		map.setUTF8Value("root/blue",         value2);
//
//		assertFalse(map.toString(),map.toString().contains("ERROR"));
//		
//		TrieParserReader reader = new TrieParserReader(3, true);
//		
//		String path = "root/red/frequency";
//		byte[] data = this.wrapping(path.getBytes(), 5);
//		
//		visitor.clearResult();
//		reader.visit(map, 
//				     visitor, 
//				     data, 0, path.length(), 31);
//		
//		System.out.println(visitor.toString());
//		
//	}
	

	@Test 
	public void testVisitorExtractBytesMiddle() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(data1, 0, 3, Integer.MAX_VALUE, value1);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(dataBytesExtractMiddle, 0, dataBytesExtractMiddle.length, Integer.MAX_VALUE, value2);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 2, 3, Integer.MAX_VALUE, value3);
		assertFalse(map.toString(),map.toString().contains("ERROR"));


		reader.visit(map, visitor,data1, 0, 3, Integer.MAX_VALUE);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); //10
		visitor.clearResult();

		reader.visit(map, visitor,data1, 2, 3, Integer.MAX_VALUE);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString()); //35
		visitor.clearResult();

		reader.visit(map, visitor,toParseMiddle, 0, toParseMiddle.length, Integer.MAX_VALUE);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); //23
		visitor.clearResult();
	}

	@Test 
	public void testVisitorExtractBytesBeginning() {
		TrieParserReader reader = new TrieParserReader(3);
		TrieParser map = new TrieParser(16);

		map.setValue(wrapping(data1,3), 0, 3, 7, value1);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(dataBytesExtractBeginning,3), 0, dataBytesExtractBeginning.length, 7, value2);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(data1,3), 2, 3, 7, value3);

		assertFalse(map.toString(),map.toString().contains("ERROR"));


		reader.visit(map, visitor,wrapping(data1,3), 0, 3, 7);//101,102,103
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); //10
		visitor.clearResult();

		reader.visit(map, visitor,wrapping(data1,3), 2, 3, 7);//103,104,105
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString()); //35
		visitor.clearResult();

		//dataBytesExtractBeginning = new byte[]{'%','b',127,100,101,102} -> mapped to 23
		reader.visit(map, visitor,wrapping(toParseBeginning,3), 0, toParseBeginning.length, 7);//10,11,12,13,127,100,101,102
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); //23 for sequential 
		visitor.clearResult();
	}

	@Test
	public void testVisitorSimpleValueReplace() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);

		map.setValue(data1, 0, 3, 7, value1); 


		reader.visit(map, visitor,data1, 0, 3, 7);//101,102,103
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); //10
		visitor.clearResult();

		map.setValue(data1, 0, 3, 7, value2);

		reader.visit(map, visitor,data1, 0, 3, 7);//101,102,103
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); //10
		visitor.clearResult();
	}

	@Test
	public void testVisitorSimpleValueReplaceWrapping() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);


		map.setValue(data1, 5, 5, 7, value1);


		reader.visit(map, visitor,data1, 5, 5, 7);//106,107,108,....
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); //10
		visitor.clearResult();

		map.setValue(data1, 5, 5, 7, value2);

		reader.visit(map, visitor,data1, 5, 5, 7);//106,107,108,....
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); //10
		visitor.clearResult();
	}

	@Test
	public void testVisitorTwoNonOverlapValuesWithReplace() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data1, 1, 3, 7, value1);
		map.setValue(data2, 1, 3, 7, value2);


		reader.visit(map, visitor,data1, 1, 3, 7);//102,103,104
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); //10
		visitor.clearResult();

		reader.visit(map, visitor,data2, 1, 3, 7);//107,108,109
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); //23
		visitor.clearResult();

		//swap values
		map.setValue(data1, 1, 3, 7, value2);
		map.setValue(data2, 1, 3, 7, value1);

		reader.visit(map, visitor,data1, 1, 3, 7);//102,103,104
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); //23
		visitor.clearResult();

		reader.visit(map, visitor,data2, 1, 3, 7);//107,108,109
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); //10
		visitor.clearResult(); 
	}

	@Test
	public void testVisitorTwoNonOverlapValuesWrappingWithReplace() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data1, 5, 5, 7, value1);
		map.setValue(data2, 5, 5, 7, value2);


		reader.visit(map, visitor,data1, 5, 5, 7);//106,107,108,....
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); 
		visitor.clearResult();

		reader.visit(map, visitor,data2, 5, 5, 7);//111,112,113,....
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); 
		visitor.clearResult();


		//swap values
		map.setValue(data1, 5, 5, 7, value2);
		map.setValue(data2, 5, 5, 7, value1);

		reader.visit(map, visitor,data1, 5, 5, 7);//106,107,108,....
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); 
		visitor.clearResult();

		reader.visit(map, visitor,data2, 5, 5, 7);//111,112,113,....
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); 
		visitor.clearResult();

	}

	@Test
	public void testVisitorTwoOverlapValues() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data2, 2, 5, 7, value1);
		map.setValue(data3, 2, 5, 7, value2);


		reader.visit(map, visitor,data2, 2, 5, 7);//108,109,110,111,112
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); 
		visitor.clearResult();

		reader.visit(map, visitor,data3, 2, 5, 7);//108,109,120,121,122
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); 
		visitor.clearResult();

		//swap values
		map.setValue(data2, 2, 5, 7, value2);
		map.setValue(data3, 2, 5, 7, value1);

		reader.visit(map, visitor,data2, 2, 5, 7);//108,109,110,111,112
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); 
		visitor.clearResult();

		reader.visit(map, visitor,data3, 2, 5, 7);//108,109,120,121,122
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); 
		visitor.clearResult();       

	}


	@Test
	public void testVisitorThreeOverlapValues() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data3, 2, 5, 7, value2);
		map.setValue(data4, 2, 5, 7, value3);
		map.setValue(data2, 2, 5, 7, value1);


		reader.visit(map, visitor,data2, 2, 5, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); 
		visitor.clearResult();

		reader.visit(map, visitor,data3, 2, 5, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); 
		visitor.clearResult();

		reader.visit(map, visitor,data4, 2, 5, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString()); 
		visitor.clearResult();

		//swap values
		map.setValue(data2, 2, 5, 7, value3);
		map.setValue(data3, 2, 5, 7, value2);
		map.setValue(data4, 2, 5, 7, value1);

		reader.visit(map, visitor,data4, 2, 5, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); 
		visitor.clearResult();

		reader.visit(map, visitor,data3, 2, 5, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data2, 2, 5, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();
	}

	@Test
	public void testVisitorInsertBeforeBranch() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data3, 0, 6, 7, value1);
		map.setValue(data4, 0, 6, 7, value2);
		map.setValue(data5, 0, 6, 7, value3);


		reader.visit(map, visitor,data3, 0, 6, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data4, 0, 6, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data5, 0, 6, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();

		//swap values
		map.setValue(data3, 0, 6, 7, value3);
		map.setValue(data4, 0, 6, 7, value2);
		map.setValue(data5, 0, 6, 7, value1);

		reader.visit(map, visitor,data5, 0, 6, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data4, 0, 6, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString()); 
		visitor.clearResult();

		reader.visit(map, visitor,data3, 0, 6, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString()); 
		visitor.clearResult();
	}

	@Test
	public void testVisitorInsertAfterBothBranchs() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data2,  1, 7, 7, value1);
		map.setValue(data3,  1, 7, 7, value2);
		map.setValue(data2b, 1, 7, 7, value3);
		map.setValue(data3b, 1, 7, 7, value4);


		reader.visit(map, visitor,data2, 1, 7, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data3, 1, 7, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data2b, 1, 7, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data3b, 1, 7, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("47", visitor.toString());
		visitor.clearResult();

		//swap values
		map.setValue(data3b, 1, 7, 7, value1);
		map.setValue(data2b, 1, 7, 7, value2);
		map.setValue(data3,  1, 7, 7, value3);
		map.setValue(data2,  1, 7, 7, value4);

		reader.visit(map, visitor,data2, 1, 7, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("47", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data3, 1, 7, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("35", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data2b, 1, 7, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data3b, 1, 7, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();      
	}


	@Test
	public void testVisitorLongInsertThenShortRootInsert() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(1000);        

		map.setValue(data1, 0, 8, 7, value1);

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 0, 3, 7, value2);

		assertFalse(map.toString(),map.toString().contains("ERROR"));


		reader.visit(map, visitor,data1, 0, 8, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data1, 0, 3, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());//23 for sequential case
		visitor.clearResult();

		//swap values
		map.setValue(data1, 0, 8, 7, value2);
		map.setValue(data1, 0, 3, 7, value1);

		reader.visit(map, visitor,data1, 0, 8, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data1, 0, 3, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();
	}

	@Test
	public void testVisitorShortRootInsertThenLongInsert() {

		TrieParserReader reader = new TrieParserReader();
		TrieParser map = new TrieParser(16);        

		map.setValue(data1, 0, 3, 7, value2); //101 102 103 -> 23

		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(data1, 0, 8, 7, value1); //101-108 -> 10

		assertFalse(map.toString(),map.toString().contains("ERROR"));


		reader.visit(map, visitor,data1, 0, 8, 7);
		System.out.println("visitor.toString() -> " + visitor.toString());
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data1, 0, 3, 7);
		System.out.println("visitor.toString() -> " + visitor.toString());
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());   //changed from 10-23
		visitor.clearResult();


		//swap values
		map.setValue(data1, 0, 3, 7, value1);
		map.setValue(data1, 0, 8, 7, value2);

		reader.visit(map, visitor,data1, 0, 8, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("23", visitor.toString());
		visitor.clearResult();

		reader.visit(map, visitor,data1, 0, 3, 7);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("10", visitor.toString()); //changed from 23 to 10
		visitor.clearResult();        
	}

	@Test
	public void testVisitorByteExtractExample() {

		TrieParserReader reader = new TrieParserReader(10);
		TrieParser map = new TrieParser(16);  

		byte[] b1 = "X-Wap-Profile:%b\r\n".getBytes();

		//  byte[] b2= "X-Online-Host:%b\r\n".getBytes();
		byte[] b2 = "Content-Length: %u\r\n".getBytes();

		byte[] b3 = "X-ATT-DeviceId:%b\r\n".getBytes();
		byte[] b4 = "X-ATT-DeviceId:%b\n".getBytes(); //testing same text with different ending

		byte[] b5 = "\r\n".getBytes(); //testing detection of empty line without capture.
		byte[] b6 = "%b\r\n".getBytes(); //testing capture of unknown pattern from the beginning

		int bits = 7;
		int mask = (1<<bits)-1;

		map.setValue(wrapping(b1,bits), 0, b1.length, mask, 1);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b2,bits), 0, b2.length, mask, 2);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b3,bits), 0, b3.length, mask, 3);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b4,bits), 0, b4.length, mask, 4); 
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b5,bits), 0, b5.length, mask, 5);
		assertFalse(map.toString(),map.toString().contains("ERROR"));

		map.setValue(wrapping(b6,bits), 0, b6.length, mask, 6);
		assertFalse(map.toString(),map.toString().contains("ERROR"));


		byte[] example = "X-Wap-Profile:ABCD\r\nHello".getBytes();
		reader.visit(map, visitor,wrapping(example,bits), 0, example.length, mask);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("", visitor.toString());//1
		visitor.clearResult();


		byte[] example1 = "Content-Length: 1234\r\n".getBytes();
		reader.visit(map, visitor,wrapping(example1,bits), 0, example1.length, mask);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("2", visitor.toString());//2 **Numeric issue
		visitor.clearResult();

		
		//byte[] b6 = "%b\r\n".getBytes() -> mapped to 6
		//byte[] b5 = "\r\n".getBytes(); -> mapped to 5.
		byte[] example6 = "\r\n".getBytes();  // wildcard of wildcard
		
		//in Data array here:  0, 2, 13, 10, 7 5. 0 is run. 2 is run length 2. 13 is /r 10 /n 7 for end and write a 5.
		
		reader.visit(map, visitor,wrapping(example6,bits), 0, example6.length, mask);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("5 6", visitor.toString());//6
		visitor.clearResult();

		
		
		byte[] example3 = "X-ATT-DeviceId:%b\r\n".getBytes(); // wildcard of wildcard
		reader.visit(map, visitor,wrapping(example3,bits), 0, example3.length, mask);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("", visitor.toString());//3
		visitor.clearResult();

		byte[] example2 = "Content-Length: %u\r\n".getBytes();// wildcard of wildcard
		reader.visit(map, visitor,wrapping(example2,bits), 0, example2.length, mask);
		assertFalse(visitor.toString(),visitor.toString().contains("ERROR"));
		assertEquals("2", visitor.toString());//2 **NUmeric issue again
		visitor.clearResult();
	}


	public static void main(String[] args) {
		//speedReadTest();
		//new TrieParserTest().testVisitorExtractBytesEndStart(); 
	}

	public static void speedReadTest() {

		TrieParserReader reader = new TrieParserReader();

		//Different values give very different results, for most small sets of URLS however it does look like the trie will be almost 2x faster than the hash.
		short testSize        = 21;//700;
		int baseSeqLen        = 40;//10;
		int maxSeqLenFromBase = 30;//180;
		int iterations        = 2000000;

		int[] testPos = new int[testSize];
		int[] testLen = new int[testSize];

		//test Data array Size will be [testSize*(baseSeqLen+maxSeqLenFromBase)] : in this case 21*(40+30) = 1470
		byte[] testData = buildTestData(testSize, baseSeqLen, maxSeqLenFromBase,testPos,testLen); 

		//Build up the ByteSequenceMap
		int maxSize = 5*testSize*(baseSeqLen+maxSeqLenFromBase); //in this case: 7350
		TrieParser bsm = new TrieParser(maxSize);
		int i;

		i = testSize;
		int expectedSum = 0;
		while (--i >= 0) {
			System.out.println("ADD:"+Arrays.toString(Arrays.copyOfRange(testData,testPos[i],testPos[i]+testLen[i])));

			bsm.setValue(testData, testPos[i], testLen[i], 0x7FFF_FFFF, i);
			expectedSum += i;

			long result = TrieParserReader.query(reader,bsm,testData, testPos[i], testLen[i], 0x7FFF_FFFF);
			if (i!=result) {
				System.err.println("unable to build expected "+i+" but got "+result);
				System.exit(0);
			}
		}
		System.out.println("done building trie limit:"+bsm.getLimit()+" max:"+maxSize);

		//Build up the classic Map
		Map<KeyBytesData, Integer> map = new HashMap<KeyBytesData, Integer>();
		i = testSize;
		while (--i >= 0) {
			map.put(new KeyBytesData(testData,testPos[i],testLen[i]), new Integer(i));
		}

		System.out.println("done with setup now run the test.");
		//ready for the read test.


		int j;

		System.out.println("exp:"+(expectedSum*iterations));

		long startTrie = System.currentTimeMillis();
		int sumTotalTrie = 0;
		j = iterations;
		while (--j>=0) {
			i = testSize;
			while (--i >= 0) {
				sumTotalTrie += TrieParserReader.query(reader,bsm,testData,testPos[i],testLen[i],0x7FFF_FFFF);
			}
		}
		long durationTrie = System.currentTimeMillis()-startTrie;
		long totalLookups = iterations*testSize;
		long lookupsPerMs = totalLookups/durationTrie;

		System.out.println("Trie duration "+durationTrie+" Lookups per MS "+lookupsPerMs);
		//header request may be between 200 and 2000 bytes, 800 is common

		int avgLookpsPerheader = 16;//this is a big guess
		long perSecond = 1000*lookupsPerMs;
		long headersPerSecond = perSecond/avgLookpsPerheader;
		long bytesPerSecond = 800 * headersPerSecond;
		System.out.println("guess of mbps read "+ (bytesPerSecond/(1024*1024)));



		long startMap = System.currentTimeMillis();
		int sumTotalMap = 0;
		j = iterations;
		while (--j>=0) {
			i = testSize;
			while (--i >= 0) {
				sumTotalMap += map.get(new KeyBytesData(testData,testPos[i],testLen[i]));
			}
		}
		long durationMap = System.currentTimeMillis()-startMap;
		System.out.println("Map duration "+durationMap);//+" sum "+sumTotalMap);

		// System.out.println(testData);

		//System.out.println("sum:"+sumTotalTrie);
		//System.out.println("sum:"+sumTotalMap);

	}

	//test data looks similar to what we will find on the pipes
	private static byte[] buildTestData(int testSize, int baseSeqLen, int maxSeqLenFromBase, int[] targetPos, int[] targetLength) {
		byte[] testData = new byte[testSize*(baseSeqLen+maxSeqLenFromBase)];

		Random r = new Random(42);

		int runningPos = 0;
		int lastPos = 0;
		int lastLength = 0;

		for(int i = 0; i<testSize; i++) {

			int activePos = 0;
			int activeLength = baseSeqLen+((i*maxSeqLenFromBase) / testSize);

			if (lastPos>0) {
				int keep = lastLength/32;
				int copyCount =  keep +  r.nextInt(lastLength-keep);
				if (copyCount>0) {
					System.arraycopy(testData, lastPos, testData, runningPos+activePos, copyCount);
					activePos += copyCount;
				}
			}
			while (activePos<activeLength) {

				byte v = 0;
				do {
					v = (byte)r.nextInt(125);
				} while ('%' == v);//eliminate the escape byte

				testData[runningPos + activePos++] = v;
			}

			lastPos = runningPos;
			lastLength = activeLength;

			targetPos[i] = lastPos;
			targetLength[i] = lastLength;

			runningPos+=activeLength;            

		}
		System.out.println("Total bytes of test data "+runningPos);
		return testData;
	}
}


/*String expected = "digraph {\n"+
"node0[label=\"BRANCH ON BIT\n"+
" bit:00001000\"]\n"+
"node0->node4\n"+
"node0->node53\n"+
"node4[label=\"RUN of 3\n"+
"klm\"]\n"+
"node4->node9\n"+
"node9[label=\"BRANCH ON BIT\n"+
" bit:00010000\"]\n"+
"node9->node13\n"+
"node9->node33\n"+
"node13[label=\"RUN of 2\n"+
"xy\"]\n"+
"node13->node17\n"+
"node17[label=\"BRANCH ON BIT\n"+
" bit:10000000\"]\n"+
"node17->node21\n"+
"node17->node27\n"+
"node21[label=\"RUN of 2\n"+
"{(-128)}{(-127)}\"]\n"+
"node21->node25\n"+
"node25[label=\"END47[26]\"]\n"+
"node27[label=\"RUN of 2\n"+
"z{\"]\n"+
"node27->node31\n"+
"node31[label=\"END23[32]\"]\n"+
"node33[label=\"RUN of 2\n"+
"no\"]\n"+
"node33->node37\n"+
"node37[label=\"BRANCH ON BIT\n"+
" bit:00000100\"]\n"+
"node37->node41\n"+
"node37->node47\n"+
"node41[label=\"RUN of 2\n"+
"vw\"]\n"+
"node41->node45\n"+
"node45[label=\"END35[46]\"]\n"+
"node47[label=\"RUN of 2\n"+
"pq\"]\n"+
"node47->node51\n"+
"node51[label=\"END10[52]\"]\n"+
"node53[label=\"RUN of 3\n"+
"efg\"]\n"+
"node53->node58\n"+
"node58[label=\"SAFE23[59], \"]\n"+
"node58->node60\n"+
"node60[label=\"RUN of 5\n"+
"hijkl\"]\n"+
"node60->node67\n"+
"node67[label=\"END10[68]\"]\n"+
"}\n";*/

//System.out.println(expected);



