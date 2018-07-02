package com.ociweb.pronghorn.util.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class LoisTest {

	int[] simpleExpectedValues = new int[] {7,1000,99999};
	
	@Test
	public void simpleInsertTest() {
		
		Lois lois = new Lois();
		lois.supportBitMaps = false;
		lois.supportRLE = false;
		
		simpleInsert(lois);		
				
	}
	
	@Test
	public void simpleInsertBitMapTest() {
		
		Lois lois = new Lois();
		lois.supportBitMaps = true;
		lois.supportRLE = false;
		
		simpleInsert(lois);		
				
	}

	private void simpleInsert(Lois lois) {
		int blockId = lois.newSet();
		
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
		
		
		assertFalse(lois.containsAny(blockId, 10, 20));
		assertFalse(lois.containsAny(blockId, 2, 7));
		assertFalse(lois.containsAny(blockId, 1000000, 1000001));
		
		assertTrue(lois.containsAny(blockId, 999,  1002));
		assertTrue(lois.containsAny(blockId, 99999, 100000));
		assertTrue(lois.containsAny(blockId, 5, 10));
		
		
		
	}
	
	@Test
	public void simpleRemoveTest() {
		
		Lois lois = new Lois();
		lois.supportBitMaps = false;
		lois.supportRLE = false;
		
		simpleRemove(lois);		
				
	}
	
	
	@Test
	public void simpleRemoveBitMapTest() {
		
		Lois lois = new Lois();
		lois.supportBitMaps = true;
		lois.supportRLE = false;
		
		simpleRemove(lois);		
				
	}

	private void simpleRemove(Lois lois) {
		int blockId = lois.newSet();
		
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
		
		largeInsertRemoval(lois);
		
	}
	
	@Test
	public void largeInsertRemovalBitMapTest() {
		
		Lois lois = new Lois();
		lois.supportBitMaps = true;
		lois.supportRLE = false;
		
		largeInsertRemoval(lois);
		
	}

	private void largeInsertRemoval(Lois lois) {
		int blockId = lois.newSet();
		
		Random r = new Random(123);
		
		int size = 9000;
		final int[] data = new int[size];
		
		for(int x = 0; x<size; x++) {
			int v = r.nextInt();
			lois.insert(blockId, v);
			data[x] = v;

			for(int y = 0; y<=x; y+=97) {
				int w = data[y];
				//add break point?
				if (!lois.containsAny(blockId, w, w+1)) {
			
					assertTrue("did not find and remove", lois.removeFromAll(w));//IT was found but not linked???
					
				}
				assertTrue("last "+x+" insert lost prev data, can not find "+w, lois.containsAny(blockId, w, w+1));
			}
			
		}
		
		final AtomicInteger pos =new AtomicInteger();
		
	
		lois.visitSet(blockId, (v)->{
			data[pos.getAndIncrement()] = v;
			assertTrue("insert has lost some data...",lois.containsAny(blockId, v, v+1));
			return true;
		});	
		
		
		
		
		//now remove a value
		//test that we no longer see the removed value
		final int neverToSeeAgain = data[5];
		lois.remove(blockId, neverToSeeAgain);
		
		lois.visitSet(blockId, (v)->{
			assertNotEquals(neverToSeeAgain, v);			
			assertTrue(lois.containsAny(blockId, v, v+1));
			
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
	
	//restore test later and fix bit map...
	@Test
	public void multipleGroupInsertBitMapTest() {
		//be sure two groups do not get mixed up
		///////////////
		Lois lois = new Lois();
		lois.supportBitMaps = true;
		lois.supportRLE = false;
		
		multipleGroupInsert(lois);	
	}
	
	//TODO: fix the bit maps
	//TODO: add tests for contains
	//TODO: then go back to image processing..
	

	private void multipleGroupInsert(Lois lois) {
		final int blockIdA = lois.newSet();
		final int blockIdB = lois.newSet();
		
		int testSize = 64; //TODO: at 64 we start to fail on binary but why???
		
		int[] expectedA = new int[testSize/2];
		int[] expectedB = new int[testSize/2];
		int posA = 0;
		int posB = 0;
				
		for(int i = 0; i<testSize; i++) {
			if (0==(1&i)) {
				lois.insert(blockIdA, i);	
				expectedA[posA++] = i;
				assertTrue("did not find "+i,lois.containsAny(blockIdA, i, i+1));
				
			} else {
				lois.insert(blockIdB, i);
				expectedB[posB++] = i;
				assertTrue("did not find "+i,lois.containsAny(blockIdB, i, i+1));
				
			}			
		}
		
		multipleGroupInsertCheck(lois, 
								blockIdA, blockIdB, 
								expectedA, expectedB);
		
		///////
		////check save and load	big block	
		
		Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(2, 1<<20);
		pipe.initBuffers();			
		while (!lois.save(pipe)) {};
		Pipe.publishEOF(pipe);//required
		
		lois = new Lois();
		while (!lois.load(pipe)) {};	
		multipleGroupInsertCheck(lois, blockIdA, blockIdB, expectedA, expectedB);
		
		///////
		////check save and load	small blocks	
		
		Pipe<RawDataSchema> pipe2 = RawDataSchema.instance.newPipe(2000, 1<<6);
		pipe2.initBuffers();			
		while (!lois.save(pipe2)) {};
		Pipe.publishEOF(pipe2);//required
		
		lois = new Lois();
		while (!lois.load(pipe2)) {};	
		multipleGroupInsertCheck(lois, blockIdA, blockIdB, expectedA, expectedB);
		
	}

	private void multipleGroupInsertCheck(Lois lois, final 
			                              int blockIdA, final int blockIdB,
			                              int[] expectedA, int[] expectedB) {
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
		
		//System.err.println(blockIdA+"  "+blockIdB+" blocks");
		
		lois.visitSetIds((v)-> {
			//System.err.println("v "+v);
			assertTrue(v==blockIdA || v==blockIdB);			
			return true;
		});
	}
	
}
