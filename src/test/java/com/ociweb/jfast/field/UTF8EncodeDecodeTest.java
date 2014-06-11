//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.Arrays;

import org.junit.Test;

import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;

public class UTF8EncodeDecodeTest {

	String unicodeTestString = new String("A" +  "\u00ea" +      "\u00f1" + "\u00fc" +
	                                      "C" +  "\u03ff" +      "\u0fff" +
			                                     "\u3fff" +      "\uffff" + "D" +	                                             
	                                             ((char)(0xf7bff))+
	                                             ((char)(0xf7bdff))+
	                                             ((char)(0xf7bfdff))+
	                                             ((char)(0xf7bffdff))+
	                                             "E"
			                             );

	@Test
	public void testUTF8EncoderCharSeqLargeBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		
		PrimitiveWriter writer = new PrimitiveWriter(4096, new FASTOutputByteArray(myData), 128, false);
		writer.writeTextUTF(unicodeTestString, writer);
		writer.flush(writer);
				
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	@Test
	public void testUTF8EncoderCharSeqTightBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		
		PrimitiveWriter writer = new PrimitiveWriter(data.length,new FASTOutputByteArray(myData),0,true);
		writer.writeTextUTF(unicodeTestString, writer);
		writer.flush(writer);
				
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	@Test
	public void testUTF8EncoderArrayLargeBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		
		PrimitiveWriter writer = new PrimitiveWriter(4096, new FASTOutputByteArray(myData), 128, false);
		char[] temp = unicodeTestString.toCharArray();
		writer.writeTextUTF(temp,0,temp.length, writer);
		writer.flush(writer);
				
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	@Test
	public void testUTF8EncoderArrayTightBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		
		PrimitiveWriter writer = new PrimitiveWriter(data.length,new FASTOutputByteArray(myData),0,true);
		char[] temp = unicodeTestString.toCharArray();
		writer.writeTextUTF(temp,0,temp.length, writer);
		writer.flush(writer);
				
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	
	@Test
	public void testUTF8DecoderArrayLargeBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		//required to have extra data to make it call the faster side of the implementation.
		int copies = 10;
		byte[] paddedData = new byte[copies*data.length];
		while (--copies>=0) {
			System.arraycopy(data, 0, paddedData, copies*data.length, data.length);
		}
		
		char[] target = new char[unicodeTestString.length()];
		
		PrimitiveReader reader = new PrimitiveReader(2048, new FASTInputByteArray(paddedData), 32);
		PrimitiveReader.fetch(reader);//required to preload the data to make it call the faster side of the implementation.
		PrimitiveReader.readTextUTF8(target, 0, unicodeTestString.length(), reader);
		
		assertTrue("chars do not match "+unicodeTestString+" vs "+new String(target), Arrays.equals(unicodeTestString.toCharArray(), target));	
		
	}
	
	@Test
	public void testUTF8DecoderArrayTightBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		char[] target = new char[unicodeTestString.length()];
		
		//TODO: when this is set too small it should throw.
		PrimitiveReader reader = new PrimitiveReader(data.length, new FASTInputByteArray(data), 0);
		PrimitiveReader.fetch(reader);
		PrimitiveReader.readTextUTF8(target, 0, unicodeTestString.length(), reader);
		
		assertTrue("chars do not match "+unicodeTestString+" vs "+new String(target), Arrays.equals(unicodeTestString.toCharArray(), target));	
		
	}
	
	
	@Test
	public void testUTF8DecoderAppendableLargeBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
				
		//required to have extra data to make it call the faster side of the implementation.
		int copies = 10;
		byte[] paddedData = new byte[copies*data.length];
		while (--copies>=0) {
			System.arraycopy(data, 0, paddedData, copies*data.length, data.length);
		}
		
		PrimitiveReader reader = new PrimitiveReader(2048, new FASTInputByteArray(paddedData), 32);
		PrimitiveReader.fetch(reader);
		String target = PrimitiveReader.readTextUTF8(unicodeTestString.length(), new StringBuilder(), reader).toString();
		
		assertTrue("chars do not match "+unicodeTestString+" vs "+target, Arrays.equals(unicodeTestString.toCharArray(), target.toCharArray()));	
		
	}
	
	@Test
	public void testUTF8DecoderAppendableTightBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		//TODO: when this is set too small it should throw.
		PrimitiveReader reader = new PrimitiveReader(data.length, new FASTInputByteArray(data), 0);
		PrimitiveReader.fetch(reader);
		String target = PrimitiveReader.readTextUTF8(/*data.length*/ unicodeTestString.length(), new StringBuilder(), reader).toString();
		
		assertEquals("chars do not match "+unicodeTestString+" vs "+target, unicodeTestString, target);	
		
	}
}
