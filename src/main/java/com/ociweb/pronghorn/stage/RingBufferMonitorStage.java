package com.ociweb.pronghorn.stage;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


public class RingBufferMonitorStage extends PronghornStage {

	private final RingBuffer observedRingBuffer;
	private final RingBuffer notifyRingBuffer;
	
	public static final int TEMPLATE_LOC;
	
	public static final int TEMPLATE_TIME_LOC;
	public static final int TEMPLATE_HEAD_LOC;
	public static final int TEMPLATE_TAIL_LOC;
	public static final int TEMPLATE_MSG_LOC;
	public static final int TEMPLATE_SIZE_LOC;
	
	static {
		FieldReferenceOffsetManager from = buildFROM();
		
		TEMPLATE_LOC = FieldReferenceOffsetManager.lookupTemplateLocator("RingStatSample", from);
		
		TEMPLATE_TIME_LOC = FieldReferenceOffsetManager.lookupFieldLocator("MS", TEMPLATE_LOC, from);
		TEMPLATE_HEAD_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Head", TEMPLATE_LOC, from);
		TEMPLATE_TAIL_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Tail", TEMPLATE_LOC, from);
		TEMPLATE_MSG_LOC = FieldReferenceOffsetManager.lookupFieldLocator("TemplateId", TEMPLATE_LOC, from);
		TEMPLATE_SIZE_LOC = FieldReferenceOffsetManager.lookupFieldLocator("BufferSize", TEMPLATE_LOC, from);
	}
	
	/**
	 * This class should be used with the ScheduledThreadPoolExecutor for 
	 * controlling the rate of samples
	 * 
	 * @param observedRingBuffer
	 * @param notifyRingBuffer
	 */
	public RingBufferMonitorStage(GraphManager gm, RingBuffer observedRingBuffer, RingBuffer notifyRingBuffer) {
		//the observed ring buffer is NOT an input
		super(gm, NONE, notifyRingBuffer); //TODO: B, should not allow stateless and no input at the same time. add assert
		this.observedRingBuffer = observedRingBuffer;
		this.notifyRingBuffer = notifyRingBuffer;
		
		FieldReferenceOffsetManager from = RingBuffer.from(notifyRingBuffer); 
		if (!from.fieldNameScript[0].equals("RingStatSample")) {
			throw new UnsupportedOperationException("Can only write to ring buffer that is expecting montior records.");
		}
		
		
		RingWriter.setPublishBatchSize(notifyRingBuffer, 0);
	}
	
	/**
	 * This FROM is provided for easy construction of RingBuffers.
	 * @return
	 */
	public static FieldReferenceOffsetManager buildFROM() {		 
		try {
			return TemplateHandler.loadFrom("/ringMonitor.xml");
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return null;
	}

	@Override
	public void run() {
		
		//if we can't write then do it again on the next cycle, and skip this data point.
		if (RingWriter.tryWriteFragment(notifyRingBuffer,TEMPLATE_LOC)) {		
			RingWriter.writeLong(notifyRingBuffer, TEMPLATE_TIME_LOC, System.currentTimeMillis());
			RingWriter.writeLong(notifyRingBuffer, TEMPLATE_HEAD_LOC, RingBuffer.headPosition(observedRingBuffer));
			RingWriter.writeLong(notifyRingBuffer, TEMPLATE_TAIL_LOC, RingBuffer.headPosition(observedRingBuffer));
			RingWriter.writeInt(notifyRingBuffer, TEMPLATE_MSG_LOC, RingReader.getMsgIdx(observedRingBuffer));	
			RingWriter.writeInt(notifyRingBuffer, TEMPLATE_SIZE_LOC, observedRingBuffer.maxSize);
			
			RingWriter.publishWrites(notifyRingBuffer);	
			
			
		}

	}

}
