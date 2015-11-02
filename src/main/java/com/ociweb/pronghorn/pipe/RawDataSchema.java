package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
public class RawDataSchema extends MessageSchema {

    public static final FieldReferenceOffsetManager FROM = FieldReferenceOffsetManager.RAW_BYTES;
    public static final RawDataSchema instance = new RawDataSchema();
    
    protected RawDataSchema() {
        super(FROM);
    }
        
}
