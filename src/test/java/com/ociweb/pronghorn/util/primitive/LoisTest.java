package com.ociweb.pronghorn.util.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class LoisTest {

	int[] simpleExpectedValues = new int[] {7,1000,99999};
	
	@Test
	public void simpleInsertTest() {
		
		Lois lois = new Lois();
		lois.supportBitMaps = false;
		lois.supportRLE = false;
		
		int blockId = lois.newBlock();
		
		boolean ok;
		ok = lois.insert(blockId, 1000);
		assertTrue(ok);
		ok = lois.insert(blockId, 7);
		assertTrue(ok);
		ok = lois.insert(blockId, 7);
		assertFalse(ok); //already inserted
		ok = lois.insert(blockId, 99999);
		assertTrue(ok);
		
		final AtomicInteger pos =new AtomicInteger();
		
		lois.visitSet(blockId, (v)->{
			assertEquals(simpleExpectedValues[pos.getAndIncrement()],v);
			
			return true;
		});		
				
	}
	
	@Test
	public void simpleRemoveTest() {
		
		Lois lois = new Lois();
		lois.supportBitMaps = false;
		lois.supportRLE = false;
		
		int blockId = lois.newBlock();
		
		boolean ok;
		ok = lois.insert(blockId, 1000);
		assertTrue(ok);
		ok = lois.insert(blockId, 7);
		assertTrue(ok);
		ok = lois.insert(blockId, 7);
		assertFalse(ok); //already inserted
		ok = lois.insert(blockId, 99999);
		assertTrue(ok);
		
		ok = lois.remove(blockId, 7);
		assertTrue(ok);
		ok = lois.remove(blockId, 7);
		assertFalse(ok);
				
		final AtomicInteger pos =new AtomicInteger();
		
		lois.visitSet(blockId, (v)->{
			if (pos.get()==0) {
				pos.getAndIncrement();//skip the removed one
			} 
			assertEquals(simpleExpectedValues[pos.getAndIncrement()],v);
			return true;
		});		
				
	}
	
	@Test
	public void largeInsertRemovalTest() {
		
		Lois lois = new Lois();
		lois.supportBitMaps = false;
		lois.supportRLE = false;
		
		int blockId = lois.newBlock();
		
		Random r = new Random(123);
		
		int size = 200;
		int x = size;
		while (--x>0) {		
			lois.insert(blockId, r.nextInt());
		}

		final int[] data = new int[size];
		final AtomicInteger pos =new AtomicInteger();
		
		lois.visitSet(blockId, (v)->{
			data[pos.getAndIncrement()] = v;
			return true;
		});	
		
		//now remove a value
		//test that we no longer see the removed value
		final int neverToSeeAgain = data[5];
		lois.remove(blockId, neverToSeeAgain);
		
		lois.visitSet(blockId, (v)->{
			assertNotEquals(neverToSeeAgain, v);
			return true;
		});
				
		////////////
		//test if the false will stop our visit in the middle.
		pos.set(0);
		lois.visitSet(blockId, (v)->{
			data[pos.getAndIncrement()] = v;
			return pos.get()<10;
		});	
		assertEquals(10, pos.get());
		
	}
	
	@Test
	public void multipleGroupInsertTest() {
		//be sure two groups do not get mixed up
		///////////////
		Lois lois = new Lois();
		lois.supportBitMaps = false;
		lois.supportRLE = false;
		
		multipleGroupInsert(lois);	
	}
	
//	//restore test later and fix bit map...
//	@Test
//	public void multipleGroupInsertBitMapTest() {
//		//be sure two groups do not get mixed up
//		///////////////
//		Lois lois = new Lois();
//		lois.supportBitMaps = true;
//		lois.supportRLE = false;
//		
//		multipleGroupInsert(lois);	
//	}

	private void multipleGroupInsert(Lois lois) {
		int blockIdA = lois.newBlock();
		int blockIdB = lois.newBlock();
		
		int[] expectedA = new int[500];
		int[] expectedB = new int[500];
		int posA = 0;
		int posB = 0;
				
		for(int i = 0; i<1000; i++) {
			if (0==(1&i)) {
				lois.insert(blockIdA, i);	
				expectedA[posA++] = i;
			} else {
				lois.insert(blockIdB, i);
				expectedB[posB++] = i;
			}			
		}
		
		final AtomicInteger pos =new AtomicInteger();
		
		lois.visitSet(blockIdA, (v)->{
			
			assertEquals(expectedA[pos.getAndIncrement()],v);
			
			return true;
		});		
		
		pos.set(0);
		lois.visitSet(blockIdB, (v)->{
			
			assertEquals(expectedB[pos.getAndIncrement()],v);
			
			return true;
		});
	}
	
}
