package com.ociweb.pronghorn.stage.monitor;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.schema.loader.TemplateHandler;

public class MonitorFROM {
	
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
}
