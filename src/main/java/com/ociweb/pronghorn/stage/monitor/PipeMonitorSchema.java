package com.ociweb.pronghorn.stage.monitor;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class PipeMonitorSchema extends MessageSchema{
	
    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc1400006,0x90800000,0x90800001,0x90800002,0x80000000,0x80200001,0xc1200006,0xc1400008,0x90800000,0x90800001,0x90800002,0x80000000,0x80200001,0x80800002,0x80200003,0xc1200008},
            (short)0,
            new String[]{"RingStatSample","MS","Head","Tail","TemplateId","BufferSize",null,"RingStatEnhancedSample","MS","Head","Tail","TemplateId","BufferSize","StackDepth","Latency",null},
            new long[]{100, 1, 2, 3, 4, 5, 0, 200, 1, 2, 3, 4, 5, 21, 22, 0},
            new String[]{"global",null,null,null,null,null,null,"global",null,null,null,null,null,null,null,null},
            "ringMonitor.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    
    public static final int MSG_RINGSTATSAMPLE_100 = 0x0;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_MS_1 = 0x2000001;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2 = 0x2000003;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3 = 0x2000005;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4 = 0x7;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5 = 0x8;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200 = 0x7;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_MS_1 = 0x2000001;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_HEAD_2 = 0x2000003;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TAIL_3 = 0x2000005;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TEMPLATEID_4 = 0x7;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_BUFFERSIZE_5 = 0x8;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_STACKDEPTH_21 = 0x9;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_LATENCY_22 = 0xa;


    public static MessageSchema instance = new PipeMonitorSchema();

    
    private PipeMonitorSchema() {
        super(FROM);
    }
	
    @Deprecated
	public static FieldReferenceOffsetManager buildFROM() {
		return FROM;
	}

}
