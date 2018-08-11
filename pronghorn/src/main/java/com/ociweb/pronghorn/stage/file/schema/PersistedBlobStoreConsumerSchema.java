package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class PersistedBlobStoreConsumerSchema extends MessageSchema<PersistedBlobStoreConsumerSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
	    new int[]{0xc0400002,0x90000000,0xc0200002,0xc0400001,0xc0200001,0xc0400001,0xc0200001},
	    (short)0,
	    new String[]{"Release","BlockId",null,"RequestReplay",null,"Clear",null},
	    new long[]{7, 3, 0, 6, 0, 12, 0},
	    new String[]{"global",null,null,"global",null,"global",null},
	    "PersistedBlobStoreConsumer.xml",
	    new long[]{2, 2, 0},
	    new int[]{2, 2, 0});


	protected PersistedBlobStoreConsumerSchema() { 
	    super(FROM);
	}

	public static final PersistedBlobStoreConsumerSchema instance = new PersistedBlobStoreConsumerSchema();

	
	public static final int MSG_RELEASE_7 = 0x00000000; //Group/OpenTempl/2
	public static final int MSG_RELEASE_7_FIELD_BLOCKID_3 = 0x00800001; //LongUnsigned/None/0
	public static final int MSG_REQUESTREPLAY_6 = 0x00000003; //Group/OpenTempl/1
	public static final int MSG_CLEAR_12 = 0x00000005; //Group/OpenTempl/1

	public static void consume(Pipe<PersistedBlobStoreConsumerSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_RELEASE_7:
	                consumeRelease(input);
	            break;
	            case MSG_REQUESTREPLAY_6:
	                consumeRequestReplay(input);
	            break;
	            case MSG_CLEAR_12:
	                consumeClear(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeRelease(Pipe<PersistedBlobStoreConsumerSchema> input) {
	    long fieldBlockId = PipeReader.readLong(input,MSG_RELEASE_7_FIELD_BLOCKID_3);
	}
	public static void consumeRequestReplay(Pipe<PersistedBlobStoreConsumerSchema> input) {
	}
	public static void consumeClear(Pipe<PersistedBlobStoreConsumerSchema> input) {
	}

	public static void publishRelease(Pipe<PersistedBlobStoreConsumerSchema> output, long fieldBlockId) {
	        PipeWriter.presumeWriteFragment(output, MSG_RELEASE_7);
	        PipeWriter.writeLong(output,MSG_RELEASE_7_FIELD_BLOCKID_3, fieldBlockId);
	        PipeWriter.publishWrites(output);
	}
	public static void publishRequestReplay(Pipe<PersistedBlobStoreConsumerSchema> output) {
	        PipeWriter.presumeWriteFragment(output, MSG_REQUESTREPLAY_6);
	        PipeWriter.publishWrites(output);
	}
	public static void publishClear(Pipe<PersistedBlobStoreConsumerSchema> output) {
	        PipeWriter.presumeWriteFragment(output, MSG_CLEAR_12);
	        PipeWriter.publishWrites(output);
	}
}
