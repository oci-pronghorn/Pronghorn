package com.ociweb.pronghorn.stage.monitor;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;


public class MonitorConsoleStage extends PronghornStage {

	private static final int SIZE_OF = Pipe.sizeOf(PipeMonitorSchema.instance, PipeMonitorSchema.MSG_RINGSTATSAMPLE_100);
	private final Pipe[] inputs;
	private GraphManager graphManager;
	private int[] percentileValues; 
	private int[] trafficValues; 
	private static final Logger logger = LoggerFactory.getLogger(MonitorConsoleStage.class);
		
	private Histogram[] hists; 
    private int position;
    private final int batchSize;
	
	private boolean recorderOn=true;
	
	private MonitorConsoleStage(GraphManager graphManager, Pipe ... inputs) {
		super(graphManager, inputs, NONE);
		this.inputs = inputs;
		this.graphManager = graphManager;
		
		this.batchSize = inputs.length>=64?64:inputs.length;
		
		validateSchema(inputs);
		GraphManager.addNota(graphManager, GraphManager.MONITOR, GraphManager.MONITOR, this);
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
		percentileValues = new int[Pipe.totalPipes()+1];
		trafficValues = new int[Pipe.totalPipes()+1];
		
		int i = inputs.length;
		hists = new Histogram[i];
		while (--i>=0) {
			hists[i] = new Histogram(100,2); 
		}
				
		position = inputs.length;
	}
		
	
	@Override
	public void run() {

		int j = batchSize; //max to check before returning thread.
		while (--j>=0) {
			if (--position<0) {
				position = inputs.length-1;
			}			
			Pipe<?> ring = inputs[position];
			
			while (Pipe.peekMsg(ring, PipeMonitorSchema.MSG_RINGSTATSAMPLE_100)) {
				int mgdIdx = Pipe.takeMsgIdx(ring);			
				long time = Pipe.takeLong(ring);
				long head = Pipe.takeLong(ring);
				long tail = Pipe.takeLong(ring); 				
				int lastMsgIdx = Pipe.takeInt(ring);
				int ringSize = Pipe.takeInt(ring);			

				if (recorderOn && head>=0 && tail>=0) {
					int pctFull = (int)((100*(head-tail))/ringSize);
					//bounds enforcement because both head and tail are snapshots and are not synchronized to one another.				
					
					hists[position].recordValue(pctFull>=0 ? (pctFull<=100 ? pctFull : 99) : 0);
				}
				
				Pipe.confirmLowLevelRead(ring, SIZE_OF);
				Pipe.releaseReadLock(ring);
				
			}
		}
	}

	@Override
	public void shutdown() {
		
		//new Exception("SHUTDOWN MonitorConsoleStage ").printStackTrace();
		
		boolean writeToConsole = true;
		
		summarizeRuntime(writeToConsole);
				
		//Send in pipe depth data	
		boolean writeImage = false;
		if (writeImage) {
			GraphManager.exportGraphDotFile(graphManager, "MonitorResults", true, percentileValues, trafficValues);
		}
	}

	protected void summarizeRuntime(boolean writeToConsole) {
		int i = hists.length;
		while (--i>=0) {
			if (null==hists[i]) {
				break;
			}
			long pctile = hists[i].getValueAtPercentile(96); //do not change: this is the 80-20 rule applied twice
			
			long avg = -1;
			if (writeToConsole) {
				try {
					avg = (long)hists[i].getMean();
				} catch (Throwable e) {
					logger.trace("unable to read mean",e);
				}
			}
			boolean inBounds = true;//value>80 || value < 1;
            long sampleCount = hists[i].getTotalCount();
            PronghornStage producer = GraphManager.getRingProducer(graphManager,  inputs[i].id);
            
            String ringName = "Unknown";
            long published = 0;
            long allocated = 0;
            if (producer instanceof PipeMonitorStage) {
            	
            	published = ((PipeMonitorStage)producer).getObservedPipePublishedCount();
            	allocated = ((PipeMonitorStage)producer).getObservedPipeBytesAllocated();
            	trafficValues[ ((PipeMonitorStage)producer).getObservedPipeId() ] = (int)published;
            	ringName = ((PipeMonitorStage)producer).getObservedPipeName();
            	
	            if (inBounds && (sampleCount>=1)) {
					percentileValues[ ((PipeMonitorStage)producer).getObservedPipeId() ] = (int)pctile;
					
	            }
            }
            if (writeToConsole) {
            	writeToConsole(i, pctile, avg, sampleCount, ringName, published, allocated);
            }
		}
	}

	private void writeToConsole(int i, long pctile, long avg, long sampleCount, String ringName, long published, long allocated) {
		while (ringName.length()<60) {
			ringName=ringName+" ";
		}            
		
		Appendables.appendValue(System.out, "    ", i, " ");
		System.out.append(ringName);
		Appendables.appendValue(System.out, " Queue Fill ", pctile, "%");
		if (avg>=0) {
			Appendables.appendValue(System.out, " Average:", avg, "%"); 
		}
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

	private static final Long defaultMonitorRate = Long.valueOf(80_000_000); //80 ms, 12.5 fps
	private static final PipeConfig defaultMonitorRingConfig = new PipeConfig(PipeMonitorSchema.instance, 15, 0);
	
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

	public void writeAsDot(GraphManager gm, Appendable payload) {
		summarizeRuntime(false);
		GraphManager.writeAsDOT(gm, payload, true, percentileValues, trafficValues);
	}

	

}
