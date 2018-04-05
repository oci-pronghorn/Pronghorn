package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class PersistedBlobLoadReleaseSchema extends MessageSchema<PersistedBlobLoadReleaseSchema> {


	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
	    new int[]{0xc0400002,0x90000000,0xc0200002},
	    (short)0,
	    new String[]{"AckRelease","BlockId",null},
	    new long[]{10, 3, 0},
	    new String[]{"global",null,null},
	    "PersistedBlobLoadRelease.xml",
	    new long[]{2, 2, 0},
	    new int[]{2, 2, 0});



	protected PersistedBlobLoadReleaseSchema() { 
	    super(FROM);
	}

	public static final PersistedBlobLoadReleaseSchema instance = new PersistedBlobLoadReleaseSchema();

	
	public static final int MSG_ACKRELEASE_10 = 0x00000000; //Group/OpenTempl/2
	public static final int MSG_ACKRELEASE_10_FIELD_BLOCKID_3 = 0x00800001; //LongUnsigned/None/0

	public static void consume(Pipe<PersistedBlobLoadReleaseSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_ACKRELEASE_10:
	                consumeAckRelease(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeAckRelease(Pipe<PersistedBlobLoadReleaseSchema> input) {
	    long fieldBlockId = PipeReader.readLong(input,MSG_ACKRELEASE_10_FIELD_BLOCKID_3);
	}

	public static void publishAckRelease(Pipe<PersistedBlobLoadReleaseSchema> output, long fieldBlockId) {
	        PipeWriter.presumeWriteFragment(output, MSG_ACKRELEASE_10);
	        PipeWriter.writeLong(output,MSG_ACKRELEASE_10_FIELD_BLOCKID_3, fieldBlockId);
	        PipeWriter.publishWrites(output);
	}

}
