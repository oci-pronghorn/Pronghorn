package com.ociweb.pronghorn.ring;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;


public class RingBufferMonitorStage implements Runnable {

	private final RingBuffer observedRingBuffer;
	private final RingBuffer notifyRingBuffer;
	
	public static final int TEMPLATE_LOC;
	
	public static final int TEMPLATE_TIME_LOC;
	public static final int TEMPLATE_HEAD_LOC;
	public static final int TEMPLATE_TAIL_LOC;
	public static final int TEMPLATE_MSG_LOC;
	
	static {
		FieldReferenceOffsetManager from = buildFROM();
		
		TEMPLATE_LOC = FieldReferenceOffsetManager.lookupTemplateLocator("RingStatSample", from);
		
		TEMPLATE_TIME_LOC = FieldReferenceOffsetManager.lookupFieldLocator("MS", TEMPLATE_LOC, from);
		TEMPLATE_HEAD_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Head", TEMPLATE_LOC, from);
		TEMPLATE_TAIL_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Tail", TEMPLATE_LOC, from);
		TEMPLATE_MSG_LOC = FieldReferenceOffsetManager.lookupFieldLocator("TemplateId", TEMPLATE_LOC, from);
	}
	
	/**
	 * This class should be used with the ScheduledThreadPoolExecutor for 
	 * controlling the rate of samples
	 * 
	 * @param observedRingBuffer
	 * @param notifyRingBuffer
	 */
	RingBufferMonitorStage(RingBuffer observedRingBuffer, RingBuffer notifyRingBuffer) {
		this.observedRingBuffer = observedRingBuffer;
		this.notifyRingBuffer = notifyRingBuffer;
		
		FieldReferenceOffsetManager from = RingBuffer.from(notifyRingBuffer); 
		if (!from.fieldNameScript[0].equals("RingStatSample")) {
			throw new UnsupportedOperationException("Can only write to ring buffer that is expecting montior records.");
		}
		
		
		RingWalker.setPublishBatchSize(notifyRingBuffer, 128);
	}
	
	/**
	 * This FROM is provided for easy construction of RingBuffers.
	 * @return
	 */
	public static FieldReferenceOffsetManager buildFROM() {
		 
		String source = "/ringMonitor.xml";
		ClientConfig clientConfig = new ClientConfig();		
		TemplateCatalogConfig catalog = new TemplateCatalogConfig(TemplateLoader.buildCatBytes(source, clientConfig ));
		return catalog.getFROM();
		
	}
	
	@Override
	public void run() {
		try {
			
			RingWalker.blockWriteFragment(notifyRingBuffer, TEMPLATE_LOC);
			
			long currentTimeMillis = System.currentTimeMillis();
			
			RingWriter.writeLong(notifyRingBuffer, currentTimeMillis);
			RingWriter.writeLong(notifyRingBuffer, RingBuffer.headPosition(observedRingBuffer));
			RingWriter.writeLong(notifyRingBuffer, RingBuffer.tailPosition(observedRingBuffer));
			RingWriter.writeInt(notifyRingBuffer, RingWalker.getMsgIdx(observedRingBuffer));
			
			RingWalker.publishWrites(notifyRingBuffer);			
			
		} catch (Throwable t) {
			RingBuffer.shutDown(notifyRingBuffer);
		}
	}

}
