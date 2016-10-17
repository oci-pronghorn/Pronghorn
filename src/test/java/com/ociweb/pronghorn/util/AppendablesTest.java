package com.ociweb.pronghorn.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AppendablesTest {

	@Test
	public void splitterTest() {
		
		String input = "/hello/this/is/text";
		
		CharSequence[] result = Appendables.split(input, '/');
				
		assertTrue("".equals(result[0]));
		assertTrue("hello".equals(result[1]));
		assertTrue("this".equals(result[2]));
		assertTrue("is".equals(result[3]));
		assertTrue("text".equals(result[4]));
		
	}
	
	
}
