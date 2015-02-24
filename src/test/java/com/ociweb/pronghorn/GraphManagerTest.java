package com.ociweb.pronghorn;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.stage.PronghornStage;

public class GraphManagerTest {

	
	
	@Test
	public void constructionOfSimpleGraph() {
		
		GraphManager gm = new GraphManager();
		
		RingBuffer rb1 = new RingBuffer(new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES));
		
		RingBuffer rb2 = new RingBuffer(new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES));
			
		PronghornStage a = new SimpleOut(gm,rb1); 
		PronghornStage b = new SimpleInOut(gm,rb1,rb2); 
		PronghornStage c = new SimpleIn(gm,rb2); 
			
		
		//
		//testing
		//
		
		assertTrue(a == GraphManager.getStage(gm,a.stageId));
		assertTrue(b == GraphManager.getStage(gm,b.stageId));
		assertTrue(c == GraphManager.getStage(gm,c.stageId));
		
		assertTrue(rb1 == GraphManager.getRing(gm,rb1.ringId));
		assertTrue(rb2 == GraphManager.getRing(gm,rb2.ringId));
		
		assertTrue(a == GraphManager.getRingProducer(gm,rb1.ringId));
		assertTrue(b == GraphManager.getRingConsumer(gm,rb1.ringId));
		assertTrue(b == GraphManager.getRingProducer(gm,rb2.ringId));
		assertTrue(c == GraphManager.getRingConsumer(gm,rb2.ringId));
		
		RingBuffer.addValue(rb1, 101); //add a single int to the ring buffer
		RingBuffer.publishWrites(rb1);

		assertTrue(GraphManager.mayHaveUpstreamData(gm, c.stageId)); //this is true because the first ring buffer has 1 integer
		
		GraphManager.terminate(gm, a);
		
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c.stageId)); //this is true because the first ring buffer has 1 integer
		
		RingBuffer.takeValue(rb1);
		RingBuffer.releaseReadLock(rb1);
		
//		TODO: AAAA, fix this test!
//		assertFalse(GraphManager.mayHaveUpstreamData(gm, c.stageId)); //this is true because the first ring buffer has 1 integer
		
		
	}
	
	@Test
	public void constructionOfForkedGraph() {
		
		GraphManager gm = new GraphManager();
		
		RingBuffer rb1 = new RingBuffer(new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES));
		
		RingBuffer rb21 = new RingBuffer(new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES));
		RingBuffer rb22 = new RingBuffer(new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES));
		
		RingBuffer rb211 = new RingBuffer(new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES));
		RingBuffer rb221 = new RingBuffer(new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES));
		
		PronghornStage a = new SimpleOut(gm,rb1); 
		PronghornStage b = new SimpleInOutSplit(gm, rb1, rb21, rb22); 
		
		PronghornStage b1 = new SimpleInOut(gm, rb21, rb211); 
		PronghornStage b2 = new SimpleInOut(gm, rb22, rb221); 
					
		PronghornStage c1 = new SimpleIn(gm, rb211); 
		PronghornStage c2 = new SimpleIn(gm, rb221);
		
		RingBuffer.addValue(rb1, 101); //add a single int to the ring buffer
		RingBuffer.publishWrites(rb1);
		
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c1.stageId)); //this is true because the first ring buffer has 1 integer
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c2.stageId)); //this is true because the first ring buffer has 1 integer
		
		GraphManager.terminate(gm, a);
				
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c1.stageId)); //this is true because the first ring buffer has 1 integer
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c2.stageId)); //this is true because the first ring buffer has 1 integer
				
		RingBuffer.takeValue(rb1);
		RingBuffer.releaseReadLock(rb1);
//		TODO: AAAA, fix this test!
//		assertFalse(GraphManager.mayHaveUpstreamData(gm, c1.stageId)); //this is true because the first ring buffer has 1 integer
//		assertFalse(GraphManager.mayHaveUpstreamData(gm, c2.stageId)); //this is true because the first ring buffer has 1 integer
//		
		
	}
	
	
	private class SimpleInOut extends PronghornStage {

		protected SimpleInOut(GraphManager pm, RingBuffer input, RingBuffer output) {
			super(pm, input, output);
		}

		@Override
		public void run() {
		}
		
	}
	
	private class SimpleInOutSplit extends PronghornStage {

		protected SimpleInOutSplit(GraphManager pm, RingBuffer input, RingBuffer ... output) {
			super(pm, input, output);
		}

		@Override
		public void run() {
		}
		
	}

	private class SimpleOut extends PronghornStage {

		protected SimpleOut(GraphManager pm, RingBuffer output) {
			super(pm, NONE, output);
		}

		@Override
		public void run() {
		}
		
	}
	
	private class SimpleIn extends PronghornStage {

		protected SimpleIn(GraphManager pm, RingBuffer input) {
			super(pm, input , NONE);
		}

		@Override
		public void run() {
		}
		
	}
	
}
