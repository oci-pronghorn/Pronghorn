package com.ociweb.pronghorn.util.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class LongDateTimeQueueTest {

	
	
	@Test
	public void simpleTest() {
		
		long a = 12345678;
		long b = 12345679;
		long c = 12346688;
		
		
		LongDateTimeQueue q = new LongDateTimeQueue(8);
		
		int h = -1;
		assertTrue((h=q.tryEnqueue(a))>=0);
		q.publishHead(h);
		assertTrue((h=q.tryEnqueue(b))>=0);
		q.publishHead(h);
		assertTrue((h=q.tryEnqueue(c))>=0);
		q.publishHead(h);
		
		assertEquals(a, q.dequeue());
		assertEquals(b, q.dequeue());
		assertEquals(c, q.dequeue());
		
		
		
		
		
	}
	
	
}
