package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
public class RawDataSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400002,0xb8000000,0xc0200002},
            (short)0,
            new String[]{"ChunkedStream","ByteArray",null},
            new long[]{1, 2, 0},
            new String[]{"global",null,null},
            "rawDataSchema.xml");
    
    public static final RawDataSchema instance = new RawDataSchema();
    
    public static final int MSG_CHUNKEDSTREAM_1 = 0x0;
    public static final int MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2 = 0x7000001;
    
    protected RawDataSchema(FieldReferenceOffsetManager from) {
        //TODO: confirm that from is a superset of FROM, Names need not match but IDs must.
        super(from);
    }
    
    protected RawDataSchema() {
        super(FROM);
    }
        
}
