package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_HEAD_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_MSG_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_SIZE_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_TAIL_LOC;
import static com.ociweb.pronghorn.stage.monitor.MonitorFROM.TEMPLATE_TIME_LOC;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RingBufferMonitorStage extends PronghornStage {

	private final RingBuffer observedRingBuffer;
	private final RingBuffer notifyRingBuffer;
	private final GraphManager gm;
		
	/**
	 * This class should be used with the ScheduledThreadPoolExecutor for 
	 * controlling the rate of samples
	 * 
	 * @param observedRingBuffer
	 * @param notifyRingBuffer
	 */
	public RingBufferMonitorStage(GraphManager gm, RingBuffer observedRingBuffer, RingBuffer notifyRingBuffer) {
		//the observed ring buffer is NOT an input
		super(gm, NONE, notifyRingBuffer); 
		this.observedRingBuffer = observedRingBuffer;
		this.notifyRingBuffer = notifyRingBuffer;
		this.gm = gm;
		
		FieldReferenceOffsetManager from = RingBuffer.from(notifyRingBuffer); 
		if (!from.fieldNameScript[0].equals("RingStatSample")) {
			throw new UnsupportedOperationException("Can only write to ring buffer that is expecting montior records.");
		}
		
		RingWriter.setPublishBatchSize(notifyRingBuffer, 0);
	}
	
	@Override
	public void run() {
		
		//if we can't write then do it again on the next cycle, and skip this data point.
		if (RingWriter.tryWriteFragment(notifyRingBuffer,TEMPLATE_LOC)) {
			
			RingWriter.writeLong(notifyRingBuffer, TEMPLATE_TIME_LOC, System.currentTimeMillis());
			RingWriter.writeLong(notifyRingBuffer, TEMPLATE_HEAD_LOC, RingBuffer.headPosition(observedRingBuffer));
			RingWriter.writeLong(notifyRingBuffer, TEMPLATE_TAIL_LOC, RingBuffer.tailPosition(observedRingBuffer));
			RingWriter.writeInt(notifyRingBuffer, TEMPLATE_MSG_LOC, RingReader.getMsgIdx(observedRingBuffer));	
			RingWriter.writeInt(notifyRingBuffer, TEMPLATE_SIZE_LOC, observedRingBuffer.sizeOfStructuredLayoutRingBuffer);
			
			RingWriter.publishWrites(notifyRingBuffer);	
		}
	}

	public String getObservedRingName() {
		//NOTE: is this really the right graph, may need to get the graph from the producer or consumer of the observedRingBuffer!!
		return GraphManager.getRingName(gm, observedRingBuffer);
	}
}
