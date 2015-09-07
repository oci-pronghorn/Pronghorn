package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_HEAD_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_MSG_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_SIZE_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_TAIL_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_TIME_LOC;

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
		
		PipeWriter.setPublishBatchSize(notifyRingBuffer, 0);
	}
	
	@Override
	public void run() {
		
		//if we can't write then do it again on the next cycle, and skip this data point.
		if (PipeWriter.tryWriteFragment(notifyRingBuffer,TEMPLATE_LOC)) {
			
			PipeWriter.writeLong(notifyRingBuffer, TEMPLATE_TIME_LOC, System.currentTimeMillis());
			PipeWriter.writeLong(notifyRingBuffer, TEMPLATE_HEAD_LOC, Pipe.headPosition(observedRingBuffer));
			PipeWriter.writeLong(notifyRingBuffer, TEMPLATE_TAIL_LOC, Pipe.tailPosition(observedRingBuffer));
			PipeWriter.writeInt(notifyRingBuffer, TEMPLATE_MSG_LOC, PipeReader.getMsgIdx(observedRingBuffer));	
			PipeWriter.writeInt(notifyRingBuffer, TEMPLATE_SIZE_LOC, observedRingBuffer.sizeOfStructuredLayoutRingBuffer);
			
			PipeWriter.publishWrites(notifyRingBuffer);	
		}
	}

	public String getObservedRingName() {
		//NOTE: is this really the right graph, may need to get the graph from the producer or consumer of the observedRingBuffer!!
		return GraphManager.getRingName(gm, observedRingBuffer);
	}
}
