package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;

import org.junit.Test;

public class AppendableBuilderTest {

	@Test
	public void loadTest() {
		
		AppendableBuilder builder = new AppendableBuilder(120);
		
		builder.append("hello");
		builder.append('w');
		builder.append("world",1,5);
		
		ByteArrayOutputStream str = new ByteArrayOutputStream();
		builder.copyTo(10, str);
		
		String actual = new String(str.toByteArray());
		assertEquals("helloworld",actual);
				
		
	}
	
	
}
