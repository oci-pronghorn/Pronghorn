//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

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

	String unicodeTestString = new String("A" + "\u00ea" + "\u00f1" + "\u00fc" + "C");

	@Test
	public void testUTF8Encoder() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		
		PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputByteArray(myData));
		pw.writeTextUTF(unicodeTestString);
		pw.flush();
				
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	
	@Test
	public void testUTF8Decoder() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		char[] target = new char[unicodeTestString.length()];
		
		PrimitiveReader pr = new PrimitiveReader(new FASTInputByteArray(data));
		pr.readTextUTF8(target, 0, unicodeTestString.length());
		
		assertTrue("chars do not match "+unicodeTestString+" vs "+new String(target), Arrays.equals(unicodeTestString.toCharArray(), target));	
		
	}
	
}
