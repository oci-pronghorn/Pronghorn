package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PipeMonitorStage extends PronghornStage {

	private final Pipe<?> observedPipe;
	private final Pipe<PipeMonitorSchema> notifyRingBuffer;
	private final GraphManager gm;
	private final String pipeName;
	private final int slabSize;
	private static final Logger logger = LoggerFactory.getLogger(PipeMonitorStage.class);
	private long dropped = 0;
	/**
	 * This class should be used with the ScheduledThreadPoolExecutor for 
	 * controlling the rate of samples
	 * 
	 * @param observedRingBuffer
	 * @param notifyRingBuffer
	 */
	public PipeMonitorStage(GraphManager gm, Pipe observedRingBuffer, Pipe<PipeMonitorSchema> notifyRingBuffer) {
		//the observed ring buffer is NOT an input
		super(gm, NONE, notifyRingBuffer); 
		this.observedPipe = observedRingBuffer;
		this.notifyRingBuffer = notifyRingBuffer;
		this.gm = gm;
		FieldReferenceOffsetManager from = Pipe.from(notifyRingBuffer); 
		if (!from.fieldNameScript[0].equals("RingStatSample")) {
			throw new UnsupportedOperationException("Can only write to ring buffer that is expecting montior records.");
		}
		GraphManager.addNota(gm, GraphManager.MONITOR, GraphManager.MONITOR, this);
		this.pipeName = GraphManager.getPipeName(gm, observedPipe).intern();
		this.slabSize = observedPipe.sizeOfSlabRing;
		
	}
	
	@Override
	public void startup() {
		Pipe.setPublishBatchSize(notifyRingBuffer, 0);//can not be done earlier 	    
	}
	
	@Override
	public void run() {
		Pipe<PipeMonitorSchema> output = notifyRingBuffer;
		Pipe<?> localObserved = observedPipe;
		
		//if we can't write then do it again on the next cycle, and skip this data point.
		if (Pipe.hasRoomForWrite(output)) {
			
			final int size = Pipe.addMsgIdx(output, MSG_RINGSTATSAMPLE_100);
			
			Pipe.addLongValue(System.currentTimeMillis(), output);
			Pipe.addLongValue(Pipe.headPosition(localObserved), output);
			Pipe.addLongValue(Pipe.tailPosition(localObserved), output);
			Pipe.addIntValue(localObserved.lastMsgIdx, output);
			Pipe.addIntValue(slabSize, output);
			Pipe.addLongValue(Pipe.totalWrittenFragments(localObserved), output);
			
			Pipe.confirmLowLevelWrite(output, size);
			Pipe.publishWrites(output);
						
		} else {
			
			//if unable to write then the values are dropped.
			if (Long.numberOfLeadingZeros(dropped)!=Long.numberOfLeadingZeros(++dropped)) {			
				logger.trace("Telemetry is not consuming collected data fast enough dropped:{} rate:{}ns  {}",dropped,(Number)GraphManager.getNota(gm, this, GraphManager.SCHEDULE_RATE, -1),output);
			}
			//if this is happening we probably have a blocking stage which does not release the thread??
			
			
		}
	}

	public String getObservedPipeName() {
		//NOTE: is this really the right graph, may need to get the graph from the producer or consumer of the observedRingBuffer!!
		return pipeName;
	}
	

	public long getObservedPipeBytesAllocated() {
		return observedPipe.config().totalBytesAllocated();
	}
	
	public int getObservedPipeId() {
		return observedPipe.id;
	}

}
