package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class PersistedBlobStoreProducerSchema extends MessageSchema<PersistedBlobStoreProducerSchema> {

public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
    new int[]{0xc0400003,0x90000000,0xb8000000,0xc0200003},
    (short)0,
    new String[]{"Block","BlockId","ByteArray",null},
    new long[]{1, 3, 2, 0},
    new String[]{"global",null,null,null},
    "PersistedBlobStoreProducer.xml",
    new long[]{2, 2, 0},
    new int[]{2, 2, 0});

public static final int MSG_BLOCK_1 = 0x00000000; //Group/OpenTempl/3
public static final int MSG_BLOCK_1_FIELD_BLOCKID_3 = 0x00800001; //LongUnsigned/None/0
public static final int MSG_BLOCK_1_FIELD_BYTEARRAY_2 = 0x01c00003; //ByteVector/None/0


protected PersistedBlobStoreProducerSchema() { 
    super(FROM);
}

public static final PersistedBlobStoreProducerSchema instance = new PersistedBlobStoreProducerSchema();


public static void consume(Pipe<PersistedBlobStoreProducerSchema> input) {
    while (PipeReader.tryReadFragment(input)) {
        int msgIdx = PipeReader.getMsgIdx(input);
        switch(msgIdx) {
            case MSG_BLOCK_1:
                consumeBlock(input);
            break;
            case -1:
               //requestShutdown();
            break;
        }
        PipeReader.releaseReadLock(input);
    }
}

public static void consumeBlock(Pipe<PersistedBlobStoreProducerSchema> input) {
    long fieldBlockId = PipeReader.readLong(input,MSG_BLOCK_1_FIELD_BLOCKID_3);
    DataInputBlobReader<PersistedBlobStoreProducerSchema> fieldByteArray = PipeReader.inputStream(input, MSG_BLOCK_1_FIELD_BYTEARRAY_2);
}

public static void publishBlock(Pipe<PersistedBlobStoreProducerSchema> output, long fieldBlockId, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength) {
        PipeWriter.presumeWriteFragment(output, MSG_BLOCK_1);
        PipeWriter.writeLong(output,MSG_BLOCK_1_FIELD_BLOCKID_3, fieldBlockId);
        PipeWriter.writeBytes(output,MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
        PipeWriter.publishWrites(output);
}
}
