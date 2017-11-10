package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;


public class HTTPRequestSchema extends MessageSchema<HTTPRequestSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400007,0x90800000,0x88000000,0x80000001,0xb8000000,0x80000002,0x80000003,0xc0200007,0xc0400006,0x90800000,0x88000000,0x88000004,0x88000005,0xb8000001,0xc0200006},
		    (short)0,
		    new String[]{"RestRequest","ChannelId","Sequence","Verb","Params","Revision","RequestContext",
		    null,"WebSocketFrame","ChannelId","Sequence","FinOpp","Mask","BinaryPayload",null},
		    new long[]{300, 21, 26, 23, 32, 24, 25, 0, 100, 21, 26, 11, 10, 12, 0},
		    new String[]{"global",null,null,null,null,null,null,null,"global",null,null,null,null,null,null},
		    "httpRequest.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


    
    private HTTPRequestSchema() {
        super(FROM);
    }

    public final static HTTPRequestSchema instance = new HTTPRequestSchema();
    
    public static final int MSG_RESTREQUEST_300 = 0x00000000; //Group/OpenTempl/7
    public static final int MSG_RESTREQUEST_300_FIELD_CHANNELID_21 = 0x00800001; //LongUnsigned/Delta/0
    public static final int MSG_RESTREQUEST_300_FIELD_SEQUENCE_26 = 0x00400003; //IntegerSigned/None/0
    public static final int MSG_RESTREQUEST_300_FIELD_VERB_23 = 0x00000004; //IntegerUnsigned/None/1
    public static final int MSG_RESTREQUEST_300_FIELD_PARAMS_32 = 0x01c00005; //ByteVector/None/0
    public static final int MSG_RESTREQUEST_300_FIELD_REVISION_24 = 0x00000007; //IntegerUnsigned/None/2
    public static final int MSG_RESTREQUEST_300_FIELD_REQUESTCONTEXT_25 = 0x00000008; //IntegerUnsigned/None/3
    public static final int MSG_WEBSOCKETFRAME_100 = 0x00000008; //Group/OpenTempl/6
    public static final int MSG_WEBSOCKETFRAME_100_FIELD_CHANNELID_21 = 0x00800001; //LongUnsigned/Delta/0
    public static final int MSG_WEBSOCKETFRAME_100_FIELD_SEQUENCE_26 = 0x00400003; //IntegerSigned/None/0
    public static final int MSG_WEBSOCKETFRAME_100_FIELD_FINOPP_11 = 0x00400004; //IntegerSigned/None/4
    public static final int MSG_WEBSOCKETFRAME_100_FIELD_MASK_10 = 0x00400005; //IntegerSigned/None/5
    public static final int MSG_WEBSOCKETFRAME_100_FIELD_BINARYPAYLOAD_12 = 0x01c00006; //ByteVector/None/1


    public static void consume(Pipe<HTTPRequestSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_RESTREQUEST_300:
                    consumeRestRequest(input);
                break;
                case MSG_WEBSOCKETFRAME_100:
                    consumeWebSocketFrame(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeRestRequest(Pipe<HTTPRequestSchema> input) {
        long fieldChannelId = PipeReader.readLong(input,MSG_RESTREQUEST_300_FIELD_CHANNELID_21);
        int fieldSequence = PipeReader.readInt(input,MSG_RESTREQUEST_300_FIELD_SEQUENCE_26);
        int fieldVerb = PipeReader.readInt(input,MSG_RESTREQUEST_300_FIELD_VERB_23);
        DataInputBlobReader<HTTPRequestSchema> fieldParams = PipeReader.inputStream(input, MSG_RESTREQUEST_300_FIELD_PARAMS_32);
        int fieldRevision = PipeReader.readInt(input,MSG_RESTREQUEST_300_FIELD_REVISION_24);
        int fieldRequestContext = PipeReader.readInt(input,MSG_RESTREQUEST_300_FIELD_REQUESTCONTEXT_25);
    }
    public static void consumeWebSocketFrame(Pipe<HTTPRequestSchema> input) {
        long fieldChannelId = PipeReader.readLong(input,MSG_WEBSOCKETFRAME_100_FIELD_CHANNELID_21);
        int fieldSequence = PipeReader.readInt(input,MSG_WEBSOCKETFRAME_100_FIELD_SEQUENCE_26);
        int fieldFinOpp = PipeReader.readInt(input,MSG_WEBSOCKETFRAME_100_FIELD_FINOPP_11);
        int fieldMask = PipeReader.readInt(input,MSG_WEBSOCKETFRAME_100_FIELD_MASK_10);
        DataInputBlobReader<HTTPRequestSchema> fieldBinaryPayload = PipeReader.inputStream(input, MSG_WEBSOCKETFRAME_100_FIELD_BINARYPAYLOAD_12);
    }

    public static void publishRestRequest(Pipe<HTTPRequestSchema> output, long fieldChannelId, int fieldSequence, int fieldVerb, byte[] fieldParamsBacking, int fieldParamsPosition, int fieldParamsLength, int fieldRevision, int fieldRequestContext) {
            PipeWriter.presumeWriteFragment(output, MSG_RESTREQUEST_300);
            PipeWriter.writeLong(output,MSG_RESTREQUEST_300_FIELD_CHANNELID_21, fieldChannelId);
            PipeWriter.writeInt(output,MSG_RESTREQUEST_300_FIELD_SEQUENCE_26, fieldSequence);
            PipeWriter.writeInt(output,MSG_RESTREQUEST_300_FIELD_VERB_23, fieldVerb);
            PipeWriter.writeBytes(output,MSG_RESTREQUEST_300_FIELD_PARAMS_32, fieldParamsBacking, fieldParamsPosition, fieldParamsLength);
            PipeWriter.writeInt(output,MSG_RESTREQUEST_300_FIELD_REVISION_24, fieldRevision);
            PipeWriter.writeInt(output,MSG_RESTREQUEST_300_FIELD_REQUESTCONTEXT_25, fieldRequestContext);
            PipeWriter.publishWrites(output);
    }
    public static void publishWebSocketFrame(Pipe<HTTPRequestSchema> output, long fieldChannelId, int fieldSequence, int fieldFinOpp, int fieldMask, byte[] fieldBinaryPayloadBacking, int fieldBinaryPayloadPosition, int fieldBinaryPayloadLength) {
            PipeWriter.presumeWriteFragment(output, MSG_WEBSOCKETFRAME_100);
            PipeWriter.writeLong(output,MSG_WEBSOCKETFRAME_100_FIELD_CHANNELID_21, fieldChannelId);
            PipeWriter.writeInt(output,MSG_WEBSOCKETFRAME_100_FIELD_SEQUENCE_26, fieldSequence);
            PipeWriter.writeInt(output,MSG_WEBSOCKETFRAME_100_FIELD_FINOPP_11, fieldFinOpp);
            PipeWriter.writeInt(output,MSG_WEBSOCKETFRAME_100_FIELD_MASK_10, fieldMask);
            PipeWriter.writeBytes(output,MSG_WEBSOCKETFRAME_100_FIELD_BINARYPAYLOAD_12, fieldBinaryPayloadBacking, fieldBinaryPayloadPosition, fieldBinaryPayloadLength);
            PipeWriter.publishWrites(output);
    }

}
