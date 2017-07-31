package com.ociweb.pronghorn.stage.file.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class PersistedBlobLoadSchema extends MessageSchema<PersistedBlobLoadSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0xb8000000,0xc0200003,0xc0400001,0xc0200001,0xc0400001,0xc0200001,0xc0400002,0x90000000,0xc0200002,0xc0400002,0x90000000,0xc0200002},
		    (short)0,
		    new String[]{"Block","BlockId","ByteArray",null,"BeginReplay",null,"FinishReplay",null,"AckRelease",
		    "BlockId",null,"AckWrite","BlockId",null},
		    new long[]{1, 3, 2, 0, 8, 0, 9, 0, 10, 3, 0, 11, 3, 0},
		    new String[]{"global",null,null,null,"global",null,"global",null,"global",null,null,"global",null,
		    null},
		    "PersistedBlobLoad.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});




	protected PersistedBlobLoadSchema() { 
	    super(FROM);
	}

	public static final PersistedBlobLoadSchema instance = new PersistedBlobLoadSchema();

	public static final int MSG_BLOCK_1 = 0x00000000;
	public static final int MSG_BLOCK_1_FIELD_BLOCKID_3 = 0x00800001;
	public static final int MSG_BLOCK_1_FIELD_BYTEARRAY_2 = 0x01c00003;
	
	public static final int MSG_BEGINREPLAY_8 = 0x00000004;
	
	public static final int MSG_FINISHREPLAY_9 = 0x00000006;
	
	public static final int MSG_ACKRELEASE_10 = 0x00000008;
	public static final int MSG_ACKRELEASE_10_FIELD_BLOCKID_3 = 0x00800001;
	
	public static final int MSG_ACKWRITE_11 = 0x0000000b;
	public static final int MSG_ACKWRITE_11_FIELD_BLOCKID_3 = 0x00800001;


	public static void consume(Pipe<PersistedBlobLoadSchema> input) {
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
	            case MSG_ACKRELEASE_10:
	                consumeAckRelease(input);
	            break;
	            case MSG_ACKWRITE_11:
	                consumeAckWrite(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeBlock(Pipe<PersistedBlobLoadSchema> input) {
	    long fieldBlockId = PipeReader.readLong(input,MSG_BLOCK_1_FIELD_BLOCKID_3);
	    ByteBuffer fieldByteArray = PipeReader.readBytes(input,MSG_BLOCK_1_FIELD_BYTEARRAY_2,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_BLOCK_1_FIELD_BYTEARRAY_2)));
	}
	public static void consumeBeginReplay(Pipe<PersistedBlobLoadSchema> input) {
	}
	public static void consumeFinishReplay(Pipe<PersistedBlobLoadSchema> input) {
	}
	public static void consumeAckRelease(Pipe<PersistedBlobLoadSchema> input) {
	    long fieldBlockId = PipeReader.readLong(input,MSG_ACKRELEASE_10_FIELD_BLOCKID_3);
	}
	public static void consumeAckWrite(Pipe<PersistedBlobLoadSchema> input) {
	    long fieldBlockId = PipeReader.readLong(input,MSG_ACKWRITE_11_FIELD_BLOCKID_3);
	}

	public static boolean publishBlock(Pipe<PersistedBlobLoadSchema> output, long fieldBlockId, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength) {
	    boolean result = false;
	    if (PipeWriter.tryWriteFragment(output, MSG_BLOCK_1)) {
	        PipeWriter.writeLong(output,MSG_BLOCK_1_FIELD_BLOCKID_3, fieldBlockId);
	        PipeWriter.writeBytes(output,MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
	        PipeWriter.publishWrites(output);
	        result = true;
	    }
	    return result;
	}
	public static boolean publishBeginReplay(Pipe<PersistedBlobLoadSchema> output) {
	    boolean result = false;
	    if (PipeWriter.tryWriteFragment(output, MSG_BEGINREPLAY_8)) {
	        PipeWriter.publishWrites(output);
	        result = true;
	    }
	    return result;
	}
	public static boolean publishFinishReplay(Pipe<PersistedBlobLoadSchema> output) {
	    boolean result = false;
	    if (PipeWriter.tryWriteFragment(output, MSG_FINISHREPLAY_9)) {
	        PipeWriter.publishWrites(output);
	        result = true;
	    }
	    return result;
	}
	public static boolean publishAckRelease(Pipe<PersistedBlobLoadSchema> output, long fieldBlockId) {
	    boolean result = false;
	    if (PipeWriter.tryWriteFragment(output, MSG_ACKRELEASE_10)) {
	        PipeWriter.writeLong(output,MSG_ACKRELEASE_10_FIELD_BLOCKID_3, fieldBlockId);
	        PipeWriter.publishWrites(output);
	        result = true;
	    }
	    return result;
	}
	public static boolean publishAckWrite(Pipe<PersistedBlobLoadSchema> output, long fieldBlockId) {
	    boolean result = false;
	    if (PipeWriter.tryWriteFragment(output, MSG_ACKWRITE_11)) {
	        PipeWriter.writeLong(output,MSG_ACKWRITE_11_FIELD_BLOCKID_3, fieldBlockId);
	        PipeWriter.publishWrites(output);
	        result = true;
	    }
	    return result;
	}


}
