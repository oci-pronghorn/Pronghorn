package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class ServerResponseSchema extends MessageSchema {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400005,0x90800000,0x88000000,0xb8000000,0x80000001,0xc0200005,0xc0400005,0x90800001,0x88000000,0xb8000000,0x80000001,0xc0200005,0xc0400002,0xb8000000,0xc0200002},
		    (short)0,
		    new String[]{"ToChannel","ChannelId","SequenceNo","Payload","RequestContext",null,"ToSubscription","SubscriptionId","SequenceNo","Payload","RequestContext",null,"Skip","Payload",null},
		    new long[]{100, 21, 23, 25, 24, 0, 200, 22, 23, 25, 24, 0, 300, 25, 0},
		    new String[]{"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null},
		    "serverResponse.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    
    public static final ServerResponseSchema instance = new ServerResponseSchema(FROM);
    
    public static final int MSG_TOCHANNEL_100 = 0x00000000;
    public static final int MSG_TOCHANNEL_100_FIELD_CHANNELID_21 = 0x00800001;
    public static final int MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23 = 0x00400003;
    public static final int MSG_TOCHANNEL_100_FIELD_PAYLOAD_25 = 0x01c00004;
    public static final int MSG_TOCHANNEL_100_FIELD_REQUESTCONTEXT_24 = 0x00000006;
    public static final int MSG_TOSUBSCRIPTION_200 = 0x00000006;
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_SUBSCRIPTIONID_22 = 0x00800001;
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_SEQUENCENO_23 = 0x00400003;
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_PAYLOAD_25 = 0x01c00004;
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_REQUESTCONTEXT_24 = 0x00000006;
    public static final int MSG_SKIP_300 = 0x0000000c;
    public static final int MSG_SKIP_300_FIELD_PAYLOAD_25 = 0x01c00001;


 
    
    private ServerResponseSchema(FieldReferenceOffsetManager from) {
        super(from);
    }

    
    
}
