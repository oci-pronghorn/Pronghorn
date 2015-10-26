package com.ociweb.pronghorn.stage.monitor;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class PipeMonitorSchema extends MessageSchema{
	
    public static final FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc1400006,0x90800000,0x90800001,0x90800002,0x80000000,0x80200001,0xc1200006,0xc1400008,0x90800000,0x90800001,0x90800002,0x80000000,0x80200001,0x80800002,0x80200003,0xc1200008},
            (short)0,
            new String[]{"RingStatSample","MS","Head","Tail","TemplateId","BufferSize",null,"RingStatEnhancedSample","MS","Head","Tail","TemplateId","BufferSize","StackDepth","Latency",null},
            new long[]{1, 1, 2, 3, 4, 5, 0, 2, 1, 2, 3, 4, 5, 21, 22, 0},
            new String[]{"global",null,null,null,null,null,null,"global",null,null,null,null,null,null,null,null},
            "ringMonitor.xml");
    
    
    public static final int MSG_RINGSTATSAMPLE_1 = 0x0;
    public static final int MSG_RINGSTATSAMPLE_1_FIELD_MS_1 = 0x2000001;
    public static final int MSG_RINGSTATSAMPLE_1_FIELD_HEAD_2 = 0x2000003;
    public static final int MSG_RINGSTATSAMPLE_1_FIELD_TAIL_3 = 0x2000005;
    public static final int MSG_RINGSTATSAMPLE_1_FIELD_TEMPLATEID_4 = 0x7;
    public static final int MSG_RINGSTATSAMPLE_1_FIELD_BUFFERSIZE_5 = 0x8;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_2 = 0x7;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_2_FIELD_MS_1 = 0x2000001;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_2_FIELD_HEAD_2 = 0x2000003;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_2_FIELD_TAIL_3 = 0x2000005;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_2_FIELD_TEMPLATEID_4 = 0x7;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_2_FIELD_BUFFERSIZE_5 = 0x8;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_2_FIELD_STACKDEPTH_21 = 0x9;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_2_FIELD_LATENCY_22 = 0xa;

    @Deprecated
    public static final int TEMPLATE_LOC = MSG_RINGSTATSAMPLE_1;
 
    @Deprecated
    public static final int TEMPLATE_TIME_LOC = MSG_RINGSTATSAMPLE_1_FIELD_MS_1;
    @Deprecated
    public static final int TEMPLATE_HEAD_LOC = MSG_RINGSTATSAMPLE_1_FIELD_HEAD_2;
    @Deprecated
    public static final int TEMPLATE_TAIL_LOC = MSG_RINGSTATSAMPLE_1_FIELD_TAIL_3;
    @Deprecated
    public static final int TEMPLATE_MSG_LOC = MSG_RINGSTATSAMPLE_1_FIELD_TEMPLATEID_4;
    @Deprecated
    public static final int TEMPLATE_SIZE_LOC = MSG_RINGSTATSAMPLE_1_FIELD_BUFFERSIZE_5;

    public static MessageSchema instance = new PipeMonitorSchema();

    
    private PipeMonitorSchema() {
        super(FROM);
    }
	
    @Deprecated
	public static FieldReferenceOffsetManager buildFROM() {
		return FROM;
	}

}
