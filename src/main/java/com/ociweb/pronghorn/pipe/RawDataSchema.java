package com.ociweb.pronghorn.pipe;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
public class RawDataSchema extends MessageSchema<RawDataSchema> {

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
    public static final int MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2 = 0x01c00001;


    public static void consume(Pipe<RawDataSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_CHUNKEDSTREAM_1:
                    consumeChunkedStream(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeChunkedStream(Pipe<RawDataSchema> input) {
        ByteBuffer fieldByteArray = PipeReader.readBytes(input,MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2)));
    }

    public static boolean publishChunkedStream(Pipe<RawDataSchema> output, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_CHUNKEDSTREAM_1)) {
            PipeWriter.writeBytes(output,MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


    
    protected RawDataSchema(FieldReferenceOffsetManager from) {
        //TODO: confirm that from is a superset of FROM, Names need not match but IDs must.
        super(from);
    }
    
    protected RawDataSchema() {
        super(FROM);
    }
    
        
}
