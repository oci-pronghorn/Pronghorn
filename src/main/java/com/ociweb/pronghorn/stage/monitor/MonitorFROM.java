package com.ociweb.pronghorn.stage.monitor;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;

public class MonitorFROM {
	
	public static final int TEMPLATE_LOC;
	
	public static final int TEMPLATE_TIME_LOC;
	public static final int TEMPLATE_HEAD_LOC;
	public static final int TEMPLATE_TAIL_LOC;
	public static final int TEMPLATE_MSG_LOC;
	public static final int TEMPLATE_SIZE_LOC;
	
	public static final FieldReferenceOffsetManager FROM;
	
	static {
				
		FROM = new FieldReferenceOffsetManager(
			    new int[]{0xc1400006,0x90800000,0x90800001,0x90800002,0x80000000,0x80200001,0xc1200006,0xc1400008,0x90800000,0x90800001,0x90800002,0x80000000,0x80200001,0x80800002,0x80200003,0xc1200008},
			    (short)0,
			    new String[]{"RingStatSample","MS","Head","Tail","TemplateId","BufferSize",null,"RingStatEnhancedSample","MS","Head","Tail","TemplateId","BufferSize","StackDepth","Latency",null},
			    new long[]{1, 1, 2, 3, 4, 5, 0, 2, 1, 2, 3, 4, 5, 21, 22, 0},
			    new String[]{"global",null,null,null,null,null,null,"global",null,null,null,null,null,null,null,null},
			    "ringMonitor.xml");
				
		TEMPLATE_LOC = FieldReferenceOffsetManager.lookupTemplateLocator("RingStatSample", FROM);
		
		TEMPLATE_TIME_LOC = FieldReferenceOffsetManager.lookupFieldLocator("MS", TEMPLATE_LOC, FROM);
		TEMPLATE_HEAD_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Head", TEMPLATE_LOC, FROM);
		TEMPLATE_TAIL_LOC = FieldReferenceOffsetManager.lookupFieldLocator("Tail", TEMPLATE_LOC, FROM);
		TEMPLATE_MSG_LOC = FieldReferenceOffsetManager.lookupFieldLocator("TemplateId", TEMPLATE_LOC, FROM);
		TEMPLATE_SIZE_LOC = FieldReferenceOffsetManager.lookupFieldLocator("BufferSize", TEMPLATE_LOC, FROM);
		
	}
	
	public static FieldReferenceOffsetManager buildFROM() {
		return FROM;
	}

}
