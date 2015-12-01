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
    
    public static final int MSG_RINGSTATSAMPLE_100 = 0x00000000;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_MS_1 = 0x00800001;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2 = 0x00800003;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3 = 0x00800005;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4 = 0x00000007;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5 = 0x00000008;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200 = 0x00000007;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_MS_1 = 0x00800001;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_HEAD_2 = 0x00800003;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TAIL_3 = 0x00800005;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TEMPLATEID_4 = 0x00000007;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_BUFFERSIZE_5 = 0x00000008;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_STACKDEPTH_21 = 0x00000009;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_LATENCY_22 = 0x0000000A;


    public static PipeMonitorSchema instance = new PipeMonitorSchema();

    
    private PipeMonitorSchema() {
        super(FROM);
    }
	
    @Deprecated
	public static FieldReferenceOffsetManager buildFROM() {
		return FROM;
	}

}
