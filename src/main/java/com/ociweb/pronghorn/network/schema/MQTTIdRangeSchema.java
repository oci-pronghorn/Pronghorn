package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class MQTTIdRangeSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400002,0x80000000,0xc0200002},
            (short)0,
            new String[]{"IdRange","Range",null},
            new long[]{1, 100, 0},
            new String[]{"global",null,null},
            "MQTTIdRanges.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});

    
    public static final MQTTIdRangeSchema instance = new MQTTIdRangeSchema();
    
    public static final int MSG_IDRANGE_1 = 0x00000000;
    public static final int MSG_IDRANGE_1_FIELD_RANGE_100 = 0x00000001;

    protected MQTTIdRangeSchema() {
        super(FROM);
    }
        
}
