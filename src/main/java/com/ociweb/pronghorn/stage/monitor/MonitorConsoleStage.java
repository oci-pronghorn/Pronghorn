package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_HEAD_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_MSG_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_SIZE_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_TAIL_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_TIME_LOC;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.util.Histogram;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


public class MonitorConsoleStage extends PronghornStage {

	private final RingBuffer[] inputs;
	private Histogram[] hists;
	
	public MonitorConsoleStage(GraphManager graphManager, RingBuffer ... inputs) {
		super(graphManager, inputs, NONE);
		this.inputs = inputs;
		
		validateSchema(inputs);
	}

	private void validateSchema(RingBuffer[] inputs) {
		int i = inputs.length;
		while (--i>=0) {
			FieldReferenceOffsetManager from = RingBuffer.from(inputs[i]); 
			if (!from.fieldNameScript[0].equals("RingStatSample")) {
				throw new UnsupportedOperationException("Can only write to ring buffer that is expecting montior records.");
			}
		}
	}

	@Override
	public void startup() {
		super.startup();
		
		int i = inputs.length;
		hists = new Histogram[i];
		while (--i>=0) {
			hists[i] = new Histogram(10000,50,0,100);
		}
	}
		
	
	@Override
	public void run() {
		

		
		
		int i = inputs.length;
		while (--i>=0) {
			
			RingBuffer ring = inputs[i];
			while (RingReader.tryReadFragment(ring)) {

				
				assert(TEMPLATE_LOC == RingReader.getMsgIdx(ring)) : "Only supporting the single monitor message type";
				

				long time = RingReader.readLong(ring, TEMPLATE_TIME_LOC);
		
				long head = RingReader.readLong(ring, TEMPLATE_HEAD_LOC);
				long tail = RingReader.readLong(ring, TEMPLATE_TAIL_LOC);
				
				int lastMsgIdx = RingReader.readInt(ring, TEMPLATE_MSG_LOC);
				int ringSize = RingReader.readInt(ring, TEMPLATE_SIZE_LOC);
				
			//	System.err.println("xxxx "+i+"   "+((100*(head-tail))/ringSize)+"  "+(head-tail)+"  "+ringSize);
				
				Histogram.sample( (100*(head-tail))/ringSize, hists[i]);
					
								
				RingReader.releaseReadLock(ring);
			}
		
			
			
		}
		
	}

	@Override
	public void shutdown() {
		
		int i = hists.length;
		while (--i>=0) {
			System.out.println("   "+i+" "+hists[i]);
		}
		super.shutdown();
	}

	

}
