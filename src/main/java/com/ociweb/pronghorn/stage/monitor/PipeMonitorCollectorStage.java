package com.ociweb.pronghorn.stage.monitor;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.AppendableByteWriter;

/**
 * Listens to all the pipe monitoring data and collects them into a single list. This is needed for the telemetry.
 */
public class PipeMonitorCollectorStage extends PronghornStage {

	private static final int SIZE_OF = Pipe.sizeOf(PipeMonitorSchema.instance, PipeMonitorSchema.MSG_RINGSTATSAMPLE_100);
	private final Pipe[] inputs;

	private int[] observedPipeId;
	
	private long[] lastFragments;
	private long[] lastTime;
	
	private GraphManager graphManager;
	
	//extract as an object
	private int[] percentileFullValues;	
	private long[] trafficValues; 
	private int[] messagesPerSecondValues;
	
	
	private static final Logger logger = LoggerFactory.getLogger(PipeMonitorCollectorStage.class);
			
	private short[] pctFull; //running exponential average of pipe percent full
	private int[]   messagesPerSecond; //running exponential average of messages per second
	
	
	
    private int position;
    private final int batchSize;
	private int reportSlowSpeed = 10;

	private PipeMonitorCollectorStage(GraphManager graphManager, Pipe ... inputs) {
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

	@Override
	public void startup() {
		super.startup();
		percentileFullValues = new int[Pipe.totalPipes()+1];
		trafficValues = new long[Pipe.totalPipes()+1];
		messagesPerSecondValues = new int[Pipe.totalPipes()+1];
		
		int i = inputs.length;
		pctFull = new short[i];
		messagesPerSecond = new int[i];
				
		position = inputs.length;
		
		observedPipeId = new int[inputs.length];
		Arrays.fill(observedPipeId, -1);
		
		lastFragments = new long[inputs.length];
		lastTime      = new long[inputs.length];
		
		
		////////////////////////////
		//What pipe is this input monitoring??
		///////////////////////////
		
		int j = inputs.length;
		while (--j>=0) {
			int monitorDataPipe = inputs[j].id;
			PronghornStage producer = GraphManager.getStage(graphManager, GraphManager.getRingProducerStageId(graphManager, monitorDataPipe));
            if (producer instanceof PipeMonitorStage) {            	            	
            	PipeMonitorStage p = (PipeMonitorStage)producer;
            	observedPipeId[j] = p.getObservedPipeForOutputId(monitorDataPipe).id;
            	
            }
            
		}
		
		
	}
		
	
	@Override
	public void run() {

		int j = batchSize; //max to check before returning thread.
		int pos = position;

		while (--j>=0) {
			if (--pos<0) {
				pos = inputs.length-1;
			}			
			//can pass in null for local hists when not gatering history
			consumeSamples(pos, inputs, pctFull, messagesPerSecond);
		}
		position = pos;
	}

	private static final int  MA_BITS = 7;
	private static final int  MA_TOTAL = 1<<MA_BITS; //128;
	private static final long MA_MULTI = MA_TOTAL-1; //127;
	
	
	//TODO: for large 8K telemetry must update this:
	//       1. add custom histogram this loop must run very fast
	//       2. add muliple MonitorConsoleStages, 1 per group of a few thousand??
	
	private void consumeSamples(int pos, Pipe[] localInputs,
			                    short[] pctFullAvg,
			                    int[] messagesPerSecond) {
		
		Pipe<?> pipe = localInputs[pos];
		long fragments = -1;
		long head = -1;
		long tail = -1;
		int ringSize = -1;
		int msgSkipped = -1;
		long time = 0;
		
		while (Pipe.hasContentToRead(pipe)) {
			msgSkipped++;
			int msgIdx = Pipe.takeMsgIdx(pipe);
			assert(PipeMonitorSchema.MSG_RINGSTATSAMPLE_100 == msgIdx);
			time = Pipe.takeLong(pipe);
			head = Pipe.takeLong(pipe);
			tail = Pipe.takeLong(pipe); 				
			int lastMsgIdx = Pipe.takeInt(pipe);
			ringSize = Pipe.takeInt(pipe);
			fragments = Pipe.takeLong(pipe);
			Pipe.confirmLowLevelRead(pipe, SIZE_OF);
			Pipe.releaseReadLock(pipe);
		}

		/////////////////////
		//to minimize monitoring work we only use the last value after reading all the data
		/////////////////////		
		if (fragments>=0) {
		
			if (msgSkipped>100 && --reportSlowSpeed>0) {			
				logger.warn("warning {} samples skipped, telemery read is not keeping up with data", msgSkipped);			
				//this should not happen unless the system is overloaded and scheduler needs to be updated.			
			}
			
			int pctFull = (int)((10000*(head-tail))/ringSize);
			pctFullAvg[pos] = (short)Math.min(9999, (((MA_MULTI*pctFullAvg[pos])+pctFull)>>>MA_BITS));
			
			//////////////////////////
			//////////compute the messages per second
			/////////////////////////
			if (lastTime[pos]!=0) {
				long period = time-lastTime[pos];
				if (period>0) {
					long messages = fragments-lastFragments[pos];
					//NOTE: extra 3 zeros of accuracy.
					long msgPerSecond = (1000_000L*messages)/period;
					//note this may be incorrect if telemetry falls behind.
							
					messagesPerSecond[pos] = (int)(((MA_MULTI*messagesPerSecond[pos])+msgPerSecond)>>>MA_BITS);
				}
				//System.err.println(messagesPerSecond[pos]);
				
			}
			lastTime[pos] = time;
			lastFragments[pos] = fragments;
			//////////////////////////////////
						
			/////////////////////////////////////
			///////////////// record the data for external use
			/////////////////////////////////////
			int pipeId = observedPipeId[pos];
			trafficValues[pipeId] = fragments; 

			messagesPerSecondValues[pipeId] = messagesPerSecond[pos];
			percentileFullValues[pipeId] = pctFullAvg[pos]/100;
			
		}
	}

	@Override
	public void shutdown() {
		
		//new Exception("SHUTDOWN MonitorConsoleStage ").printStackTrace();

					
		//Send in pipe depth data	
		boolean writeImage = false;
		if (writeImage) {
			GraphManager.exportGraphDotFile(graphManager, "MonitorResults", true, percentileFullValues, trafficValues, messagesPerSecondValues);
		}
	}

	private static final Long defaultMonitorRate = Long.valueOf(GraphManager.TELEMTRY_SERVER_RATE); 
	private static final PipeConfig defaultMonitorRingConfig = new PipeConfig(PipeMonitorSchema.instance, 15, 0);
	
	public static PipeMonitorCollectorStage attach(GraphManager gm) {
		return attach(gm,defaultMonitorRate,defaultMonitorRingConfig);
	}
	
	public static PipeMonitorCollectorStage attach(GraphManager gm, long rate) {
	        return attach(gm,Long.valueOf(rate),defaultMonitorRingConfig);
	}
	
	/**
	 * Easy entry point for adding monitoring to the graph.  This should be copied by all the other monitor consumers.  TODO: build for JMX, SLF4J, Socket.io
	 * @param gm
	 * @param monitorRate
	 * @param ringBufferMonitorConfig
	 */
	public static PipeMonitorCollectorStage attach(GraphManager gm, Long monitorRate, PipeConfig ringBufferMonitorConfig) {

		PipeMonitorCollectorStage stage = new PipeMonitorCollectorStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig));
        
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, monitorRate>>5, stage);
		GraphManager.addNota(gm, GraphManager.MONITOR, "dummy", stage);
		return stage;
	}

	public void writeAsDot(GraphManager gm, String name, AppendableByteWriter<?> payload) {
		
		GraphManager.writeAsDOT(gm, name, payload, true, percentileFullValues, trafficValues, messagesPerSecondValues);
	}

	public void writeAsSummary(GraphManager gm, AppendableByteWriter<?> payload) {
		GraphManager.writeAsSummary(gm, payload, percentileFullValues);
	}	

}
