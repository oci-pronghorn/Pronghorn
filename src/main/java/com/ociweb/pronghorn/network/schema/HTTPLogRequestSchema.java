package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;


public class HTTPLogRequestSchema extends MessageSchema<HTTPLogRequestSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
			new int[]{0xc0400005,0x90000000,0x90800001,0x88000000,0xa8000000,0xc0200005},
			(short)0,
			new String[]{"Request","Time","ChannelId","Sequence","Head",null},
			new long[]{1, 11, 12, 13, 14, 0},
			new String[]{"global",null,null,null,null,null},
			"HTTPLogRequest.xml",
			new long[]{2, 2, 0},
			new int[]{2, 2, 0});

	public HTTPLogRequestSchema(FieldReferenceOffsetManager from) { 
	    super(from);
	}

	public HTTPLogRequestSchema() { 
	    super(FROM);
	}

	public static final HTTPLogRequestSchema instance = new HTTPLogRequestSchema();


	public static final int MSG_REQUEST_1 = 0x00000000; //Group/OpenTempl/5
	public static final int MSG_REQUEST_1_FIELD_TIME_11 = 0x00800001; //LongUnsigned/None/0
	public static final int MSG_REQUEST_1_FIELD_CHANNELID_12 = 0x00800003; //LongUnsigned/Delta/1
	public static final int MSG_REQUEST_1_FIELD_SEQUENCE_13 = 0x00400005; //IntegerSigned/None/0
	public static final int MSG_REQUEST_1_FIELD_HEAD_14 = 0x01400006; //UTF8/None/0

	public static void consume(Pipe<HTTPLogRequestSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_REQUEST_1:
	                consumeRequest(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeRequest(Pipe<HTTPLogRequestSchema> input) {
	    long fieldTime = PipeReader.readLong(input,MSG_REQUEST_1_FIELD_TIME_11);
	    long fieldChannelId = PipeReader.readLong(input,MSG_REQUEST_1_FIELD_CHANNELID_12);
	    int fieldSequence = PipeReader.readInt(input,MSG_REQUEST_1_FIELD_SEQUENCE_13);
	    StringBuilder fieldHead = PipeReader.readUTF8(input,MSG_REQUEST_1_FIELD_HEAD_14,new StringBuilder(PipeReader.readBytesLength(input,MSG_REQUEST_1_FIELD_HEAD_14)));
	}

	public static void publishRequest(Pipe<HTTPLogRequestSchema> output, long fieldTime, long fieldChannelId, int fieldSequence, CharSequence fieldHead) {
	        PipeWriter.presumeWriteFragment(output, MSG_REQUEST_1);
	        PipeWriter.writeLong(output,MSG_REQUEST_1_FIELD_TIME_11, fieldTime);
	        PipeWriter.writeLong(output,MSG_REQUEST_1_FIELD_CHANNELID_12, fieldChannelId);
	        PipeWriter.writeInt(output,MSG_REQUEST_1_FIELD_SEQUENCE_13, fieldSequence);
	        PipeWriter.writeUTF8(output,MSG_REQUEST_1_FIELD_HEAD_14, fieldHead);
	        PipeWriter.publishWrites(output);
	}
}
