package com.ociweb.pronghorn.pipe.util.hash;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class LongHashSetTest {

	@Test
	public void testThis() {
		
		LongHashSet set = new LongHashSet(8);
		
		LongHashSet.setItem(set, 123);
		
		assertTrue(LongHashSet.hasItem(set, 123));
		assertFalse(LongHashSet.hasItem(set, 4123));
		LongHashSet.clear(set);
		assertFalse(LongHashSet.hasItem(set, 123));
		
	}
}
