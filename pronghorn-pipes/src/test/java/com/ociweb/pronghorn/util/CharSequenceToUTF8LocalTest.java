package com.ociweb.pronghorn.util;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class CharSequenceToUTF8LocalTest {

	@Test
	public void testAppendToBytes() {
		String username="nathan";
		String password="password";
		byte[] data = CharSequenceToUTF8Local.get().convert(username).append(":").append(password).asBytes();
		
		assertTrue(Arrays.equals((username+":"+password).getBytes(), data));
	}
	
}
