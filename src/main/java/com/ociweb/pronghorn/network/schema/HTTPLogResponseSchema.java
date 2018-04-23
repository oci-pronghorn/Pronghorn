package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;


public class HTTPLogResponseSchema extends MessageSchema<HTTPLogResponseSchema> {


	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
	    new int[]{0xc0400006,0x90000000,0x90800001,0x88000000,0xa8000000,0x90000002,0xc0200006},
	    (short)0,
	    new String[]{"Response","Time","ChannelId","Sequence","Head","Duration",null},
	    new long[]{1, 11, 12, 13, 14, 15, 0},
	    new String[]{"global",null,null,null,null,null,null},
	    "HTTPLogResponse.xml",
	    new long[]{2, 2, 0},
	    new int[]{2, 2, 0});
	
	
	public HTTPLogResponseSchema(FieldReferenceOffsetManager from) { 
	    super(from);
	}

	public HTTPLogResponseSchema() { 
	    super(FROM);
	}

	public static final HTTPLogResponseSchema instance = new HTTPLogResponseSchema();


	public static final int MSG_RESPONSE_1 = 0x00000000; //Group/OpenTempl/6
	public static final int MSG_RESPONSE_1_FIELD_TIME_11 = 0x00800001; //LongUnsigned/None/0
	public static final int MSG_RESPONSE_1_FIELD_CHANNELID_12 = 0x00800003; //LongUnsigned/Delta/1
	public static final int MSG_RESPONSE_1_FIELD_SEQUENCE_13 = 0x00400005; //IntegerSigned/None/0
	public static final int MSG_RESPONSE_1_FIELD_HEAD_14 = 0x01400006; //UTF8/None/0
	public static final int MSG_RESPONSE_1_FIELD_DURATION_15 = 0x00800008; //LongUnsigned/None/2

	public static void consume(Pipe<HTTPLogResponseSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_RESPONSE_1:
	                consumeResponse(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeResponse(Pipe<HTTPLogResponseSchema> input) {
	    long fieldTime = PipeReader.readLong(input,MSG_RESPONSE_1_FIELD_TIME_11);
	    long fieldChannelId = PipeReader.readLong(input,MSG_RESPONSE_1_FIELD_CHANNELID_12);
	    int fieldSequence = PipeReader.readInt(input,MSG_RESPONSE_1_FIELD_SEQUENCE_13);
	    StringBuilder fieldHead = PipeReader.readUTF8(input,MSG_RESPONSE_1_FIELD_HEAD_14,new StringBuilder(PipeReader.readBytesLength(input,MSG_RESPONSE_1_FIELD_HEAD_14)));
	    long fieldDuration = PipeReader.readLong(input,MSG_RESPONSE_1_FIELD_DURATION_15);
	}

	public static void publishResponse(Pipe<HTTPLogResponseSchema> output, long fieldTime, long fieldChannelId, int fieldSequence, CharSequence fieldHead, long fieldDuration) {
	        PipeWriter.presumeWriteFragment(output, MSG_RESPONSE_1);
	        PipeWriter.writeLong(output,MSG_RESPONSE_1_FIELD_TIME_11, fieldTime);
	        PipeWriter.writeLong(output,MSG_RESPONSE_1_FIELD_CHANNELID_12, fieldChannelId);
	        PipeWriter.writeInt(output,MSG_RESPONSE_1_FIELD_SEQUENCE_13, fieldSequence);
	        PipeWriter.writeUTF8(output,MSG_RESPONSE_1_FIELD_HEAD_14, fieldHead);
	        PipeWriter.writeLong(output,MSG_RESPONSE_1_FIELD_DURATION_15, fieldDuration);
	        PipeWriter.publishWrites(output);
	}
}
