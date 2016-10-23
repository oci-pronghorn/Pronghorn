package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class ClientNetResponseSchema extends MessageSchema {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0xb8000000,0xc0200003,0xc0400003,0x90000000,0xa8000001,0xc0200003},
		    (short)0,
		    new String[]{"Response","ConnectionId","Payload",null,"SimpleResponse","ConnectionId","Payload",null},
		    new long[]{200, 201, 203, 0, 210, 201, 204, 0},
		    new String[]{"global",null,null,null,"global",null,null,null},
		    "ClientNetResponse.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    
    public static final ClientNetResponseSchema instance = new ClientNetResponseSchema();
    
    public static final int MSG_RESPONSE_200 = 0x00000000;
    public static final int MSG_RESPONSE_200_FIELD_CONNECTIONID_201 = 0x00800001;
    public static final int MSG_RESPONSE_200_FIELD_PAYLOAD_203 = 0x01C00003;
    
    public static final int MSG_SIMPLERESPONSE_210 = 0x00000004;
    public static final int MSG_SIMPLERESPONSE_210_FIELD_CONNECTIONID_201 = 0x00800001;
    public static final int MSG_SIMPLERESPONSE_210_FIELD_PAYLOAD_204 = 0x01400003;
    
    protected ClientNetResponseSchema() {
        super(FROM);
    }
        
}
