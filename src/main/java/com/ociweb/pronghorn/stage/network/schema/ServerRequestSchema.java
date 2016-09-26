package com.ociweb.pronghorn.stage.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class ServerRequestSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400003,0x90800000,0xb8000000,0xc0200003},
            (short)0,
            new String[]{"FromChannel","ChannelId","Payload",null},
            new long[]{100, 21, 25, 0},
            new String[]{"global",null,null,null},
            "serverRequest.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    
    public static final ServerRequestSchema instance = new ServerRequestSchema(FROM);
    
    public static final int MSG_FROMCHANNEL_100 = 0x00000000;
    public static final int MSG_FROMCHANNEL_100_FIELD_CHANNELID_21 = 0x00800001;
    public static final int MSG_FROMCHANNEL_100_FIELD_PAYLOAD_25 = 0x01C00003;

 

    
    private ServerRequestSchema(FieldReferenceOffsetManager from) {
        super(from);
    }

    
    
}
