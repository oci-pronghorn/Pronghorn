package com.ociweb.pronghorn.mqtt.schmea;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class MQTTIdRangeSchema extends MessageSchema {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0x90000001,0xc0200003,0xc0400004,0x90000000,0x90000001,0x88000000,0xc0200004},
		    (short)0,
		    new String[]{"Release","ConnectionID","Position",null,"ReleaseWithSeq","ConnectionID","Position","SequenceNo",null},
		    new long[]{100, 1, 2, 0, 101, 1, 2, 3, 0},
		    new String[]{"global",null,null,null,"global",null,null,null,null},
		    "Release.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});

    
    public static final MQTTIdRangeSchema instance = new MQTTIdRangeSchema();
    
    public static final int MSG_RELEASE_100 = 0x00000000;
    public static final int MSG_RELEASE_100_FIELD_CONNECTIONID_1 = 0x00800001;
    public static final int MSG_RELEASE_100_FIELD_POSITION_2 = 0x00800003;
    public static final int MSG_RELEASEWITHSEQ_101 = 0x00000004;
    public static final int MSG_RELEASEWITHSEQ_101_FIELD_CONNECTIONID_1 = 0x00800001;
    public static final int MSG_RELEASEWITHSEQ_101_FIELD_POSITION_2 = 0x00800003;
    public static final int MSG_RELEASEWITHSEQ_101_FIELD_SEQUENCENO_3 = 0x00400005;

    protected MQTTIdRangeSchema() {
        super(FROM);
    }
        
}
