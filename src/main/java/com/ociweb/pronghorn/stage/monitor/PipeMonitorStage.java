package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.*;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PipeMonitorStage extends PronghornStage {

	private final Pipe observedPipe;
	private final Pipe notifyRingBuffer;
	private final GraphManager gm;
	private final String pipeName;
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
	}
	
	@Override
	public void startup() {
		Pipe.setPublishBatchSize(notifyRingBuffer, 0);//can not be done earlier 	    
	}
	
	@Override
	public void run() {
		Pipe output = notifyRingBuffer;
		Pipe input = observedPipe;
		//if we can't write then do it again on the next cycle, and skip this data point.
		if (Pipe.hasRoomForWrite(output)) {
			
			int size = Pipe.addMsgIdx(output, MSG_RINGSTATSAMPLE_100);
			
			Pipe.addLongValue(System.currentTimeMillis(), output);
			Pipe.addLongValue(Pipe.headPosition(input), output);
			Pipe.addLongValue(Pipe.tailPosition(input), output);
			Pipe.addIntValue(input.lastMsgIdx, output);
			Pipe.addIntValue(input.sizeOfSlabRing, output);
		
			Pipe.confirmLowLevelWrite(output, size);
			Pipe.publishWrites(output);
			
		} else {
			//if unable to write then the values are dropped.
		}
	}

	public String getObservedPipeName() {
		//NOTE: is this really the right graph, may need to get the graph from the producer or consumer of the observedRingBuffer!!
		return pipeName;
	}
	
	public long getObservedPipePublishedCount() {
		return Pipe.headPosition(observedPipe);
	}
	
	public long getObservedPipeBytesAllocated() {
		return observedPipe.config().totalBytesAllocated();
	}
	
	public int getObservedPipeId() {
		return observedPipe.id;
	}
}
