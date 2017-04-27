package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;


public class HTTPRequestSchema extends MessageSchema<HTTPRequestSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400007,0x90800000,0x88000000,0x80000001,0xb8000000,0x80000002,0x80000003,0xc0200007},
		    (short)0,
		    new String[]{"RestRequest","ChannelId","Sequence","Verb","Params","Revision","RequestContext",
		    null},
		    new long[]{300, 21, 26, 23, 32, 24, 25, 0},
		    new String[]{"global",null,null,null,null,null,null,null},
		    "httpRequest.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


    
    private HTTPRequestSchema() {
        super(FROM);
    }

    public final static HTTPRequestSchema instance = new HTTPRequestSchema();
    
    public static final int MSG_RESTREQUEST_300 = 0x00000000;
    public static final int MSG_RESTREQUEST_300_FIELD_CHANNELID_21 = 0x00800001;
    public static final int MSG_RESTREQUEST_300_FIELD_SEQUENCE_26 = 0x00400003;
    public static final int MSG_RESTREQUEST_300_FIELD_VERB_23 = 0x00000004;
    public static final int MSG_RESTREQUEST_300_FIELD_PARAMS_32 = 0x01c00005;
    public static final int MSG_RESTREQUEST_300_FIELD_REVISION_24 = 0x00000007;
    public static final int MSG_RESTREQUEST_300_FIELD_REQUESTCONTEXT_25 = 0x00000008;


    public static void consume(Pipe<HTTPRequestSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_RESTREQUEST_300:
                    consumeRestRequest(input);
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
        ByteBuffer fieldParams = PipeReader.readBytes(input,MSG_RESTREQUEST_300_FIELD_PARAMS_32,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_RESTREQUEST_300_FIELD_PARAMS_32)));
        int fieldRevision = PipeReader.readInt(input,MSG_RESTREQUEST_300_FIELD_REVISION_24);
        int fieldRequestContext = PipeReader.readInt(input,MSG_RESTREQUEST_300_FIELD_REQUESTCONTEXT_25);
    }

    public static boolean publishRestRequest(Pipe<HTTPRequestSchema> output, long fieldChannelId, int fieldSequence, int fieldVerb, byte[] fieldParamsBacking, int fieldParamsPosition, int fieldParamsLength, int fieldRevision, int fieldRequestContext) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_RESTREQUEST_300)) {
            PipeWriter.writeLong(output,MSG_RESTREQUEST_300_FIELD_CHANNELID_21, fieldChannelId);
            PipeWriter.writeInt(output,MSG_RESTREQUEST_300_FIELD_SEQUENCE_26, fieldSequence);
            PipeWriter.writeInt(output,MSG_RESTREQUEST_300_FIELD_VERB_23, fieldVerb);
            PipeWriter.writeBytes(output,MSG_RESTREQUEST_300_FIELD_PARAMS_32, fieldParamsBacking, fieldParamsPosition, fieldParamsLength);
            PipeWriter.writeInt(output,MSG_RESTREQUEST_300_FIELD_REVISION_24, fieldRevision);
            PipeWriter.writeInt(output,MSG_RESTREQUEST_300_FIELD_REQUESTCONTEXT_25, fieldRequestContext);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


}
