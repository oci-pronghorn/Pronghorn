package com.ociweb.jfast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.Arrays;

import org.junit.Test;

public class MyCharSequenceTest {

	String unicodeTestString = new String("A" + "\u00ea" + "\u00f1" + "\u00fc" + "C");

	@Test
	public void testUTF8Encoder() {
		MyCharSequnce mcs = new MyCharSequnce();
		 
		char[] charData = unicodeTestString.toCharArray();
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		mcs.encodeUTF8(charData, 0, unicodeTestString.length(), myData, 0);
		
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	
	@Test
	public void testUTF8Decoder() {
		MyCharSequnce mcs = new MyCharSequnce();
		
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));

		
		System.err.println(unicodeTestString+" encoded as "+data.length+" bytes "+Arrays.toString(data));
		
		//TODO: still not working.
		
		char[] target = new char[unicodeTestString.length()];
		mcs.decodeUTF8(data, 0, target, 0, target.length);
		
		System.err.println(new String(target));
		
		
	}
	
}
