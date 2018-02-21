package com.ociweb.pronghorn.stage.monitor;

import java.util.Arrays;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.AppendableBuilder;
import com.ociweb.pronghorn.util.Appendables;


public class MonitorConsoleStage extends PronghornStage {

	private static final int SIZE_OF = Pipe.sizeOf(PipeMonitorSchema.instance, PipeMonitorSchema.MSG_RINGSTATSAMPLE_100);
	private final Pipe[] inputs;

	private int[] observedPipeId;
	private long[] observedPipeBytesAllocated;
	private String[] observedPipeName;
	
	private GraphManager graphManager;
	private int[] percentileValues; 
	private long[] trafficValues; 
	private static final Logger logger = LoggerFactory.getLogger(MonitorConsoleStage.class);
	
	private final boolean writeToConsoleOnShutdown = false;	
		
	private Histogram[] hists;
	private short[] pctFull;
    private int position;
    private final int batchSize;
	private int reportSlowSpeed = 10;
    
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
		trafficValues = new long[Pipe.totalPipes()+1];
		
		int i = inputs.length;
		pctFull = new short[i];
		hists = new Histogram[i];
		while (--i>=0) {
			hists[i] = new Histogram(10000,2); 
		}
				
		position = inputs.length;
		
		observedPipeId = new int[inputs.length];
		Arrays.fill(observedPipeId, -1);
		observedPipeBytesAllocated = new long[inputs.length];
		observedPipeName = new String[inputs.length];
		
		int j = inputs.length;
		while (--j>=0) {
			int stageId = GraphManager.getRingProducerStageId(graphManager, inputs[j].id);	
            PronghornStage producer = GraphManager.getStage(graphManager, stageId);
            if (producer instanceof PipeMonitorStage) {
            	PipeMonitorStage p = (PipeMonitorStage)producer;
            	
            	observedPipeId[j] = p.getObservedPipeId();
            	observedPipeBytesAllocated[j] = p.getObservedPipeBytesAllocated();
            	observedPipeName[j] = p.getObservedPipeName();
            	
            }
            
		}
		
		
	}
		
	
	@Override
	public void run() {

		int j = batchSize; //max to check before returning thread.
		int pos = position;
		Histogram[] localHists = recorderOn ? hists : null;
		
		while (--j>=0) {
			if (--pos<0) {
				pos = inputs.length-1;
			}			
			//can pass in null for local hists when not gatering history
			consumeSamples(pos, inputs, localHists, pctFull);
		}
		position = pos;
	}

	//TODO: for large 8K telemetry must update this:
	//       1. add custom histogram this loop must run very fast
	//       2. add muliple MonitorConsoleStages, 1 per group of a few thousand??
	
	private void consumeSamples(int pos, Pipe[] localInputs, Histogram[] localHists, short[] pctFullAvg) {
		Pipe<?> pipe = localInputs[pos];
		long consumed = -1;
		long head = -1;
		long tail = -1;
		int ringSize = -1;
		int msgSkipped = -1;
		
		while (Pipe.hasContentToRead(pipe)) {
			msgSkipped++;
			int msgIdx = Pipe.takeMsgIdx(pipe);
			assert(PipeMonitorSchema.MSG_RINGSTATSAMPLE_100 == msgIdx);
			long time = Pipe.takeLong(pipe);
			head = Pipe.takeLong(pipe);
			tail = Pipe.takeLong(pipe); 				
			int lastMsgIdx = Pipe.takeInt(pipe);
			ringSize = Pipe.takeInt(pipe);
		   
			consumed = Pipe.takeLong(pipe);
			
			Pipe.confirmLowLevelRead(pipe, SIZE_OF);
			Pipe.releaseReadLock(pipe);
		}

		/////////////////////
		//to minimize monitoring work we only use the last value after reading all the data
		/////////////////////		
		if (consumed>=0) {
			
			recordPctFull(pos, localHists, pctFullAvg, consumed, head, tail, ringSize, msgSkipped); 
		}
	}

	private void recordPctFull(int pos, Histogram[] localHists, short[] pctFullAvg, long consumed, long head, long tail,
			int ringSize, int msgSkipped) {
		if (msgSkipped>10 && --reportSlowSpeed>0) {			
			logger.warn("warning {} samples skipped, telemery read is not keeping up with data", msgSkipped);			
			//this should not happen unless the system is overloaded and scheduler needs to be updated.			
		}
		
		int pctFull = (int)((10000*(head-tail))/ringSize);
		if (null!=localHists && head>=0 && tail>=0) {
			//bounds enforcement because both head and tail are snapshots and are not synchronized to one another.				
			localHists[pos].recordValue(pctFull>=0 ? (pctFull<=10000 ? pctFull : 9999) : 0);
		}
		pctFullAvg[pos] = (short)Math.min(9999, (((99*pctFullAvg[pos])+pctFull)/100));
					
		trafficValues[this.observedPipeId[pos]] = consumed;
	}

	@Override
	public void shutdown() {
		
		//new Exception("SHUTDOWN MonitorConsoleStage ").printStackTrace();

		
		summarizeRuntime(writeToConsoleOnShutdown, ValueType.Percentile96th);
				
		//Send in pipe depth data	
		boolean writeImage = false;
		if (writeImage) {
			GraphManager.exportGraphDotFile(graphManager, "MonitorResults", true, percentileValues, trafficValues);
		}
	}

	protected void summarizeRuntime(boolean writeToConsole, 
			                        ValueType pipePctFullType) {
		Histogram[] localHists = hists;
		int[] localPercentileValues = percentileValues;
		
		int i = localHists.length;
		while (--i>=0) {
			if (null==localHists[i]) {
				continue;
			}
			long pctile = 0;
			
			
			//TOOD: change to pass in percentile instead of enum...
			switch (pipePctFullType) {
				case Maxium:
					pctile = localHists[i].getMaxValue()/10000;
					break;
				case NearRealTime:
					pctile = pctFull[i]/100;
					break;
				case Percentile96th:
					pctile = localHists[i].getValueAtPercentile(96)/10000; //do not change: this is the 80-20 rule applied twice
					break;
			}
			
			long avg = accumAvg(writeToConsole, localHists, i);
			boolean inBounds = true;//value>80 || value < 1;
            long sampleCount = localHists[i].getTotalCount();
                        
            String ringName = "Unknown";
            long published = 0;
            long allocated = 0;
            if (observedPipeId[i]>=0) {
            	
            	allocated = observedPipeBytesAllocated[i];
            	ringName = observedPipeName[i];
            	            	
	            if (inBounds && (sampleCount>=1)) {
	            	localPercentileValues[observedPipeId[i]] = (int)pctile;
	            }
            }
            if (writeToConsole) {
            	writeToConsole(i, pctile, avg, sampleCount, ringName, published, allocated);
            }
		}
	}

	private long accumAvg(boolean writeToConsole, Histogram[] localHists, int i) {
		long avg = -1;
		if (writeToConsole) {
			try {
				avg = (long)localHists[i].getMean();
			} catch (Throwable e) {
				logger.trace("unable to read mean",e);
			}
		}
		return avg;
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

	private static final Long defaultMonitorRate = Long.valueOf(GraphManager.TELEMTRY_SERVER_RATE); 
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
        
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, monitorRate>>5, stage);
		GraphManager.addNota(gm, GraphManager.MONITOR, "dummy", stage);
		return stage;
	}

	public void writeAsDot(GraphManager gm, AppendableBuilder payload) {
		summarizeRuntime(false, ValueType.NearRealTime);

	//	ServerCoordinator.newDotRequestStart = System.nanoTime();
		GraphManager.writeAsDOT(gm, payload, true, percentileValues, trafficValues);

		
		
	}

	

}
