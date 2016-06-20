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
            "rawDataSchema.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    
    public static final RawDataSchema instance = new RawDataSchema();
    
    public static final int MSG_CHUNKEDSTREAM_1 = 0x00000000;
    public static final int MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2 = 0x01C00001;
    
    // ByteArray DataInputBlobReader DataOutputBlobWriter
    // String    CharSequence        Appendable
    // int       int                 int
    
    public interface RawDataConsumer {
        void consume(DataInputBlobReader value);    
    }
    
    public interface RawDataProducer {
        void produce(DataOutputBlobWriter value);
    }
    
    protected RawDataSchema(FieldReferenceOffsetManager from) {
        //TODO: confirm that from is a superset of FROM, Names need not match but IDs must.
        super(from);
    }
    
    protected RawDataSchema() {
        super(FROM);
    }
    
        
}
