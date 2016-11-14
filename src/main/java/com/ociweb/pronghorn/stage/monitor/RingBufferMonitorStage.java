package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.*;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RingBufferMonitorStage extends PronghornStage {

	private final Pipe observedRingBuffer;
	private final Pipe notifyRingBuffer;
	private final GraphManager gm;
		
	/**
	 * This class should be used with the ScheduledThreadPoolExecutor for 
	 * controlling the rate of samples
	 * 
	 * @param observedRingBuffer
	 * @param notifyRingBuffer
	 */
	public RingBufferMonitorStage(GraphManager gm, Pipe observedRingBuffer, Pipe notifyRingBuffer) {
		//the observed ring buffer is NOT an input
		super(gm, NONE, notifyRingBuffer); 
		this.observedRingBuffer = observedRingBuffer;
		this.notifyRingBuffer = notifyRingBuffer;
		this.gm = gm;
		
		FieldReferenceOffsetManager from = Pipe.from(notifyRingBuffer); 
		if (!from.fieldNameScript[0].equals("RingStatSample")) {
			throw new UnsupportedOperationException("Can only write to ring buffer that is expecting montior records.");
		}
	}
	
	@Override
	public void startup() {
	    PipeWriter.setPublishBatchSize(notifyRingBuffer, 0);//can not be done earlier 	    
	}
	
	@Override
	public void run() {
		//if we can't write then do it again on the next cycle, and skip this data point.
		if (PipeWriter.tryWriteFragment(notifyRingBuffer, MSG_RINGSTATSAMPLE_100)) {
			
			PipeWriter.writeLong(notifyRingBuffer, MSG_RINGSTATSAMPLE_100_FIELD_MS_1, System.currentTimeMillis());
			PipeWriter.writeLong(notifyRingBuffer, MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2, Pipe.headPosition(observedRingBuffer));
			PipeWriter.writeLong(notifyRingBuffer, MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3, Pipe.tailPosition(observedRingBuffer));
			PipeWriter.writeInt(notifyRingBuffer, MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4, observedRingBuffer.lastMsgIdx);	
			PipeWriter.writeInt(notifyRingBuffer, MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5, observedRingBuffer.sizeOfSlabRing);
			
			PipeWriter.publishWrites(notifyRingBuffer);
			assert(Pipe.headPosition(notifyRingBuffer)==Pipe.workingHeadPosition(notifyRingBuffer)) : "publish did not clean up, is the publish batching? it should not.";
			
		}
	}

	public String getObservedRingName() {
		//NOTE: is this really the right graph, may need to get the graph from the producer or consumer of the observedRingBuffer!!
		return GraphManager.getRingName(gm, observedRingBuffer);
	}
	
	public long getObservedRingPublishedCount() {
		return Pipe.headPosition(observedRingBuffer);
	}
	
	public int getObservedRingId() {
		return observedRingBuffer.id;
	}
}
