package com.ociweb.pronghorn.stage.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema;


public class HTTPRequestSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400007,0x90800000,0x80000000,0x80000001,0xb8000000,0x80000002,0x80000003,0xc0200007,0xc0400007,0x90800000,0x80000000,0x80000001,0xb8000001,0x80000002,0x80000003,0xc0200007},
            (short)0,
            new String[]{"FileRequest","ChannelId","Sequence","Verb","ByteArray","Revision","RequestContext",null,"RestRequest","ChannelId","Sequence","Verb","Params","Revision","RequestContext",null},
            new long[]{200, 21, 26, 23, 22, 24, 25, 0, 300, 21, 26, 23, 32, 24, 25, 0},
            new String[]{"global",null,null,null,null,null,null,null,"global",null,null,null,null,null,null,null},
            "httpRequest.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});

    
    public final static HTTPRequestSchema instance = new HTTPRequestSchema();
    
    public static final int MSG_FILEREQUEST_200 = 0x00000000;
    public static final int MSG_FILEREQUEST_200_FIELD_CHANNELID_21 = 0x00800001;
    public static final int MSG_FILEREQUEST_200_FIELD_SEQUENCE_26 = 0x00000003;
    public static final int MSG_FILEREQUEST_200_FIELD_VERB_23 = 0x00000004;
    public static final int MSG_FILEREQUEST_200_FIELD_BYTEARRAY_22 = 0x01C00005;
    public static final int MSG_FILEREQUEST_200_FIELD_REVISION_24 = 0x00000007;
    public static final int MSG_FILEREQUEST_200_FIELD_REQUESTCONTEXT_25 = 0x00000008;
    
    public static final int MSG_RESTREQUEST_300 = 0x00000008;
    public static final int MSG_RESTREQUEST_300_FIELD_CHANNELID_21 = 0x00800001;
    public static final int MSG_RESTREQUEST_300_FIELD_SEQUENCE_26 = 0x00000003;
    public static final int MSG_RESTREQUEST_300_FIELD_VERB_23 = 0x00000004;
    public static final int MSG_RESTREQUEST_300_FIELD_PARAMS_32 = 0x01C00005;
    public static final int MSG_RESTREQUEST_300_FIELD_REVISION_24 = 0x00000007;
    public static final int MSG_RESTREQUEST_300_FIELD_REQUESTCONTEXT_25 = 0x00000008;
    
    private HTTPRequestSchema() {
        super(FROM);
    }

}
