package com.ociweb.pronghorn.util.primitive;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ByteArrayQueueTest {

	@Test
	public void simpleExampleTest() {
		
		byte[] a = "asdfesaf".getBytes();
		byte[] b = "asdfesad".getBytes();
		byte[] c = "asdfexxxx".getBytes();
				
		ByteArrayQueue q = new ByteArrayQueue(10,1024);
				
		assertTrue(q.tryEnqueue(a, Integer.MAX_VALUE, 0, a.length));
		assertTrue(q.tryEnqueue(b, Integer.MAX_VALUE, 0, b.length));
		assertTrue(q.tryEnqueue(c, Integer.MAX_VALUE, 0, c.length));
		
		
		byte[] resultA = new byte[a.length];
		int lenA = q.dequeue(resultA, Integer.MAX_VALUE, 0);
		
		byte[] resultB = new byte[b.length];
		int lenB = q.dequeue(resultB, Integer.MAX_VALUE, 0);
		
		byte[] resultC = new byte[c.length];
		int lenC = q.dequeue(resultC, Integer.MAX_VALUE, 0);
		
		
	}
	
	
}
