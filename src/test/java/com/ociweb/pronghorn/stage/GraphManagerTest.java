package com.ociweb.pronghorn.stage;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class GraphManagerTest {

	
	
	@Test
	public void constructionOfSimpleGraph() {
		
		GraphManager gm = new GraphManager();
		
		Pipe rb1 = new Pipe(new PipeConfig(FieldReferenceOffsetManager.RAW_BYTES));
		rb1.initBuffers();
		
		Pipe rb2 = new Pipe(new PipeConfig(FieldReferenceOffsetManager.RAW_BYTES));
		rb2.initBuffers();
		
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
		
		Pipe.addIntValue(101, rb1); //add a single int to the ring buffer
		Pipe.publishWrites(rb1);

		assertTrue(GraphManager.mayHaveUpstreamData(gm, c.stageId)); //this is true because the first ring buffer has 1 integer
		
		GraphManager.setStateToStopping(gm, a.stageId);
		
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c.stageId)); //this is true because the first ring buffer has 1 integer
				
		Pipe.releaseReads(rb1);
		GraphManager.setStateToStopping(gm, a.stageId);
		GraphManager.setStateToStopping(gm, b.stageId);

		assertTrue(GraphManager.mayHaveUpstreamData(gm, c.stageId)); //this is true because the first ring buffer has 1 integer
		
		
	}
	
	@Test
	public void constructionOfForkedGraph() {
		
		GraphManager gm = new GraphManager();
		
		Pipe rb1 = new Pipe(new PipeConfig(FieldReferenceOffsetManager.RAW_BYTES));
		rb1.initBuffers();
		Pipe rb21 = new Pipe(new PipeConfig(FieldReferenceOffsetManager.RAW_BYTES));
		rb21.initBuffers();
		Pipe rb22 = new Pipe(new PipeConfig(FieldReferenceOffsetManager.RAW_BYTES));
		rb22.initBuffers();
		
		Pipe rb211 = new Pipe(new PipeConfig(FieldReferenceOffsetManager.RAW_BYTES));
		rb211.initBuffers();
		
		Pipe rb221 = new Pipe(new PipeConfig(FieldReferenceOffsetManager.RAW_BYTES));
		rb221.initBuffers();
		
		PronghornStage a = new SimpleOut(gm,rb1); 
		PronghornStage b = new SimpleInOutSplit(gm, rb1, rb21, rb22); 
		
		PronghornStage b1 = new SimpleInOut(gm, rb21, rb211); 
		PronghornStage b2 = new SimpleInOut(gm, rb22, rb221); 
					
		PronghornStage c1 = new SimpleIn(gm, rb211); 
		PronghornStage c2 = new SimpleIn(gm, rb221);
		
		Pipe.addIntValue(101, rb1); //add a single int to the ring buffer
		Pipe.publishWrites(rb1);
		
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c1.stageId)); //this is true because the first ring buffer has 1 integer
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c2.stageId)); //this is true because the first ring buffer has 1 integer
		
		GraphManager.setStateToStopping(gm, a.stageId);
				
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c1.stageId)); //this is true because the first ring buffer has 1 integer
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c2.stageId)); //this is true because the first ring buffer has 1 integer
				
		Pipe.releaseReads(rb1);
		
		GraphManager.setStateToStopping(gm, a.stageId);
		GraphManager.setStateToStopping(gm, b.stageId);
		GraphManager.setStateToStopping(gm, b1.stageId);
		GraphManager.setStateToStopping(gm, b2.stageId);
		
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c1.stageId)); //this is true because the first ring buffer has 1 integer
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c2.stageId)); //this is true because the first ring buffer has 1 integer

		
	}
	
	
	private class SimpleInOut extends PronghornStage {

		protected SimpleInOut(GraphManager pm, Pipe input, Pipe output) {
			super(pm, input, output);
		}

		@Override
		public void run() {
		}
		
	}
	
	private class SimpleInOutSplit extends PronghornStage {

		protected SimpleInOutSplit(GraphManager pm, Pipe input, Pipe ... output) {
			super(pm, input, output);
		}

		@Override
		public void run() {
		}
		
	}

	private class SimpleOut extends PronghornStage {

		protected SimpleOut(GraphManager pm, Pipe output) {
			super(pm, NONE, output);
		}

		@Override
		public void run() {
		}
		
	}
	
	private class SimpleIn extends PronghornStage {

		protected SimpleIn(GraphManager pm, Pipe input) {
			super(pm, input , NONE);
		}

		@Override
		public void run() {
		}
		
	}
	
}
