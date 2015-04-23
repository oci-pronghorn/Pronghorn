package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_HEAD_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_MSG_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_SIZE_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_TAIL_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_TIME_LOC;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.util.Histogram;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


public class MonitorConsoleStage extends PronghornStage {

	private final RingBuffer[] inputs;
	private Histogram[] hists;
	private GraphManager graphManager;
	
	public MonitorConsoleStage(GraphManager graphManager, RingBuffer ... inputs) {
		super(graphManager, inputs, NONE);
		this.inputs = inputs;
		this.graphManager = graphManager;
		
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
			hists[i] = new Histogram(1000,50,0,100);
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
				
				long pctFull = (100*(head-tail))/ringSize;
				//bounds enforcement because both head and tail are snapshots and are not synchronized to one another.				
				Histogram.sample( pctFull>=0 ? pctFull<=100 ? pctFull : 99 : 0, hists[i]);
													
				RingReader.releaseReadLock(ring);
			}
		
			
			
		}
		
	}

	@Override
	public void shutdown() {
		
		
		//TODO: AA, may want to flat zeros if they are on the down stream of router? or switch?
		
		int i = hists.length;
		while (--i>=0) {
			
			long value = hists[i].valueAtPercent(.5);
			
			if ((value>80 || value < 1 ) && (Histogram.sampleCount(hists[i])>2)) {
				PronghornStage producer = GraphManager.getRingProducer(graphManager,  inputs[i].ringId);
				//NOTE: may need to walk up tree till we find this object, (future feature)
				String ringName;
				if (producer instanceof RingBufferMonitorStage) {
					ringName = ((RingBufferMonitorStage)producer).getObservedRingName();
				} else {
					ringName = "Unknown";
				}
					
				System.out.println("    "+i+" "+ringName+" Queue Fill Median:"+value+"% Average:"+(Histogram.accumulatedTotal(hists[i])/Histogram.sampleCount(hists[i]))+"%");
			}
		}
	}

	private static final Integer defaultMonitorRate = Integer.valueOf(50000000);
	private static final RingBufferConfig defaultMonitorRingConfig = new RingBufferConfig(MonitorFROM.buildFROM(), 30, 0);
	
	public static void attach(GraphManager gm) {
		attach(gm,defaultMonitorRate,defaultMonitorRingConfig);
	}
	
	
	/**
	 * Easy entry point for adding monitoring to the graph.  This should be copied by all the other monitor consumers.  TODO: build for JMX, SLF4J, Socket.io
	 * @param gm
	 * @param monitorRate
	 * @param ringBufferMonitorConfig
	 */
	public static void attach(GraphManager gm, Integer monitorRate, RingBufferConfig ringBufferMonitorConfig) {
		MonitorConsoleStage stage = new MonitorConsoleStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig));
        GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, monitorRate, stage);
		GraphManager.addAnnotation(gm, GraphManager.MONITOR, "dummy", stage);
		
	}

	

}
