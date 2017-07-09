package com.ociweb.pronghorn.pipe;

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

    protected RawDataSchema(FieldReferenceOffsetManager from) {
        super(from);
    }
    
    protected RawDataSchema() {
        super(FROM);
    }
    
    public static final int MSG_CHUNKEDSTREAM_1 = 0x00000000; //Group/OpenTempl/2
    public static final int MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2 = 0x01c00001; //ByteVector/None/0


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
        DataInputBlobReader<RawDataSchema> fieldByteArray = PipeReader.inputStream(input, MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
    }

    public static void publishChunkedStream(Pipe<RawDataSchema> output, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength) {
            PipeWriter.presumeWriteFragment(output, MSG_CHUNKEDSTREAM_1);
            PipeWriter.writeBytes(output,MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
            PipeWriter.publishWrites(output);
    }

        
}
