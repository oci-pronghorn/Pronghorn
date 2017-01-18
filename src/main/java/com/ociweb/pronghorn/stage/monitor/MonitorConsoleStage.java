package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_MS_1;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3;
import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4;

import org.HdrHistogram.Histogram;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;


public class MonitorConsoleStage extends PronghornStage {

	private final Pipe[] inputs;
	private GraphManager graphManager;
	private int[] percentileValues; 
	private int[] trafficValues; 
		
	private Histogram[] hists; 

	private boolean recorderOn=true;
	
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
	
	public void setRecorderOn(boolean isOn) {
		this.recorderOn = isOn;
	}
	public boolean isRecorderOn() {
		return this.recorderOn;
	}

	@Override
	public void startup() {
		super.startup();
		percentileValues = new int[Pipe.totalRings()+1];
		trafficValues = new int[Pipe.totalRings()+1];
		
		int p = Thread.currentThread().getPriority();
		Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
		int i = inputs.length;
		hists = new Histogram[i];
		while (--i>=0) {
			hists[i] = new Histogram(100,2); 
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
				
				if (recorderOn) {
					hists[i].recordValue(pctFull>=0 ? pctFull<=100 ? pctFull : 99 : 0);
				}
				PipeReader.releaseReadLock(ring);
			}
		}
	}

	@Override
	public void shutdown() {
		
		int i = hists.length;
		while (--i>=0) {
			
			long pctile = hists[i].getValueAtPercentile(80); 
			long avg = (long)hists[i].getMean();
				
			boolean inBounds = true;//value>80 || value < 1;
            long sampleCount = hists[i].getTotalCount();
            PronghornStage producer = GraphManager.getRingProducer(graphManager,  inputs[i].id);
            
            String ringName = "Unknown";
            long published = 0;
            long allocated = 0;
            if (producer instanceof RingBufferMonitorStage) {
            	
            	published = ((RingBufferMonitorStage)producer).getObservedPipePublishedCount();
            	allocated = ((RingBufferMonitorStage)producer).getObservedPipeBytesAllocated();
            	trafficValues[ ((RingBufferMonitorStage)producer).getObservedPipeId() ] = (int)published;
            	ringName = ((RingBufferMonitorStage)producer).getObservedPipeName();
            	
	            if (inBounds && (sampleCount>=1)) {
					percentileValues[ ((RingBufferMonitorStage)producer).getObservedPipeId() ] = (int)avg;
					
	            }
            }
            
            writeToConsole(i, pctile, avg, sampleCount, ringName, published, allocated);
            
		}
				
		//Send in pipe depth data		
		GraphManager.exportGraphDotFile(graphManager, "MonitorResults", percentileValues, trafficValues);
		
	}

	private void writeToConsole(int i, long pctile, long avg, long sampleCount, String ringName, long published, long allocated) {
		while (ringName.length()<60) {
			ringName=ringName+" ";
		}            
		
		Appendables.appendValue(System.out, "    ", i, " ");
		System.out.append(ringName);
		Appendables.appendValue(System.out, " Queue Fill ", pctile, "%");
		Appendables.appendValue(System.out, " Average:", avg, "%"); 
		Appendables.appendValue(System.out, "    samples:", sampleCount);
		Appendables.appendValue(System.out, "  totalPublished:",published);
		
		if (allocated>(1<<30)) {
			int gb = (int)(allocated>>30);
			Appendables.appendValue(System.out, "  allocated:",gb,"GB");
		} else {
			if (allocated>(1<<20)) {
				int mb = (int)(allocated>>20);
				Appendables.appendValue(System.out, "  allocated:",mb,"MB");				
			} else {
				if (allocated>(1<<10)) {
					int kb = (int)(allocated>>10);
					Appendables.appendValue(System.out, "  allocated:",kb,"KB");
				} else {
					Appendables.appendValue(System.out, "  allocated:",allocated,"B");
				}
			}
		}
		System.out.println();
		
		
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
