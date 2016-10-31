package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_MS_1;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.util.Histogram;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


public class MonitorConsoleStage extends PronghornStage {

	private final Pipe[] inputs;
	private Histogram[] hists; ///TODO: replace with HDRHistogram
	private GraphManager graphManager;
	
	
	public MonitorConsoleStage(GraphManager graphManager, Pipe ... inputs) {
		super(graphManager, inputs, NONE);
		this.inputs = inputs;
		this.graphManager = graphManager;
		
		validateSchema(inputs);
	}

	private void validateSchema(Pipe[] inputs) {
		int i = inputs.length;
		while (--i>=0) {
			FieldReferenceOffsetManager from = Pipe.from(inputs[i]); 
			if (!from.fieldNameScript[0].equals("RingStatSample")) {
				throw new UnsupportedOperationException("Can only write to ring buffer that is expecting montior records.");
			}
		}
	}

	@Override
	public void startup() {
		super.startup();
		int p = Thread.currentThread().getPriority();
		Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
		int i = inputs.length;
		hists = new Histogram[i];
		while (--i>=0) {
			hists[i] = new Histogram(1000,50,0,100);
		}
		Thread.currentThread().setPriority(p);
	}
		
	
	@Override
	public void run() {
		
		int i = inputs.length;
		while (--i>=0) {
			
			Pipe ring = inputs[i];
			while (PipeReader.tryReadFragment(ring)) {

				
				assert(MSG_RINGSTATSAMPLE_100 == PipeReader.getMsgIdx(ring)) : "Only supporting the single monitor message type";
				

				long time = PipeReader.readLong(ring, MSG_RINGSTATSAMPLE_100_FIELD_MS_1);
		
				long head = PipeReader.readLong(ring, MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2);
				long tail = PipeReader.readLong(ring, MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3);
				
				int lastMsgIdx = PipeReader.readInt(ring, MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4);
				int ringSize = PipeReader.readInt(ring, MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5);
				
				long pctFull = (100*(head-tail))/ringSize;
				//bounds enforcement because both head and tail are snapshots and are not synchronized to one another.				
				Histogram.sample( pctFull>=0 ? pctFull<=100 ? pctFull : 99 : 0, hists[i]);
													
				PipeReader.releaseReadLock(ring);
			}
		}
	}

	@Override
	public void shutdown() {
		
		int i = hists.length;
		while (--i>=0) {
			
			long value = hists[i].valueAtPercent(.5);
			
			boolean inBounds = true;//value>80 || value < 1;
            long sampleCount = Histogram.sampleCount(hists[i]);
            if (inBounds && (sampleCount>=2)) {
				PronghornStage producer = GraphManager.getRingProducer(graphManager,  inputs[i].id);
				//NOTE: may need to walk up tree till we find this object, (future feature)
				String ringName;
				long published = 0;
				if (producer instanceof RingBufferMonitorStage) {
					ringName = ((RingBufferMonitorStage)producer).getObservedRingName();
					published = ((RingBufferMonitorStage)producer).getObservedRingPublishedCount();
				} else {
					ringName = "Unknown";
				}
				
				while (ringName.length()<60) {
					ringName=ringName+" ";
				}
				
				System.out.println("    "+i+" "+ringName+" Queue Fill Median:"+value+"% Average:"+(Histogram.accumulatedTotal(hists[i])/sampleCount)+"%    samples:"+sampleCount+"  totalPublished:"+published);
			}
		}
	}

	private static final Long defaultMonitorRate = Long.valueOf(50000000);
	private static final PipeConfig defaultMonitorRingConfig = new PipeConfig(PipeMonitorSchema.instance, 30, 0);
	
	public static MonitorConsoleStage attach(GraphManager gm) {
		return attach(gm,defaultMonitorRate,defaultMonitorRingConfig);
	}
	
	public static MonitorConsoleStage attach(GraphManager gm, long rate) {
	        return attach(gm,Long.valueOf(rate),defaultMonitorRingConfig);
	}
	
	/**
	 * Easy entry point for adding monitoring to the graph.  This should be copied by all the other monitor consumers.  TODO: build for JMX, SLF4J, Socket.io
	 * @param gm
	 * @param monitorRate
	 * @param ringBufferMonitorConfig
	 */
	public static MonitorConsoleStage attach(GraphManager gm, Long monitorRate, PipeConfig ringBufferMonitorConfig) {
		MonitorConsoleStage stage = new MonitorConsoleStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig));
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, monitorRate, stage);
		GraphManager.addNota(gm, GraphManager.MONITOR, "dummy", stage);
		return stage;
	}

	

}
