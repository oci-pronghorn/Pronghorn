package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class PersistedBlobLoadConsumerSchema extends MessageSchema<PersistedBlobLoadConsumerSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
	    new int[]{0xc0400003,0x90000000,0xb8000000,0xc0200003,0xc0400001,0xc0200001,0xc0400001,0xc0200001},
	    (short)0,
	    new String[]{"Block","BlockId","ByteArray",null,"BeginReplay",null,"FinishReplay",null},
	    new long[]{1, 3, 2, 0, 8, 0, 9, 0},
	    new String[]{"global",null,null,null,"global",null,"global",null},
	    "PersistedBlobLoadConsumer.xml",
	    new long[]{2, 2, 0},
	    new int[]{2, 2, 0});

	
	protected PersistedBlobLoadConsumerSchema(FieldReferenceOffsetManager from) { 
		super(from);
	}
	
	protected PersistedBlobLoadConsumerSchema() { 
		super(FROM);
	}
	
	public static final PersistedBlobLoadConsumerSchema instance = new PersistedBlobLoadConsumerSchema();

	public static final int MSG_BLOCK_1 = 0x00000000; //Group/OpenTempl/3
	public static final int MSG_BLOCK_1_FIELD_BLOCKID_3 = 0x00800001; //LongUnsigned/None/0
	public static final int MSG_BLOCK_1_FIELD_BYTEARRAY_2 = 0x01c00003; //ByteVector/None/0
	public static final int MSG_BEGINREPLAY_8 = 0x00000004; //Group/OpenTempl/1
	public static final int MSG_FINISHREPLAY_9 = 0x00000006; //Group/OpenTempl/1

	public static void consume(Pipe<PersistedBlobLoadConsumerSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_BLOCK_1:
	                consumeBlock(input);
	            break;
	            case MSG_BEGINREPLAY_8:
	                consumeBeginReplay(input);
	            break;
	            case MSG_FINISHREPLAY_9:
	                consumeFinishReplay(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeBlock(Pipe<PersistedBlobLoadConsumerSchema> input) {
	    long fieldBlockId = PipeReader.readLong(input,MSG_BLOCK_1_FIELD_BLOCKID_3);
	    DataInputBlobReader<PersistedBlobLoadConsumerSchema> fieldByteArray = PipeReader.inputStream(input, MSG_BLOCK_1_FIELD_BYTEARRAY_2);
	}
	public static void consumeBeginReplay(Pipe<PersistedBlobLoadConsumerSchema> input) {
	}
	public static void consumeFinishReplay(Pipe<PersistedBlobLoadConsumerSchema> input) {
	}

	public static void publishBlock(Pipe<PersistedBlobLoadConsumerSchema> output, long fieldBlockId, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength) {
	        PipeWriter.presumeWriteFragment(output, MSG_BLOCK_1);
	        PipeWriter.writeLong(output,MSG_BLOCK_1_FIELD_BLOCKID_3, fieldBlockId);
	        PipeWriter.writeBytes(output,MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
	        PipeWriter.publishWrites(output);
	}
	public static void publishBeginReplay(Pipe<PersistedBlobLoadConsumerSchema> output) {
	        PipeWriter.presumeWriteFragment(output, MSG_BEGINREPLAY_8);
	        PipeWriter.publishWrites(output);
	}
	public static void publishFinishReplay(Pipe<PersistedBlobLoadConsumerSchema> output) {
	        PipeWriter.presumeWriteFragment(output, MSG_FINISHREPLAY_9);
	        PipeWriter.publishWrites(output);
	}


}
