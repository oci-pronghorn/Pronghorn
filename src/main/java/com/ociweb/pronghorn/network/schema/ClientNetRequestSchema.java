package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class ClientNetRequestSchema extends MessageSchema {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0xa8000000,0xc0200003,0xc0400002,0x90000000,0xc0200002,0xc0400003,0x90000000,0xb8000001,0xc0200003},
		    (short)0,
		    new String[]{"SimpleRequest","ConnectionId","Payload",null,"SimpleDisconnect","ConnectionId",null,"EncryptedRequest","ConnectionId","Encrypted",null},
		    new long[]{100, 101, 103, 0, 101, 101, 0, 110, 101, 104, 0},
		    new String[]{"global",null,null,null,"global",null,null,"global",null,null,null},
		    "ClientNetRequest.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    
    public static final ClientNetRequestSchema instance = new ClientNetRequestSchema();
    
    public static final int MSG_SIMPLEREQUEST_100 = 0x00000000;
    public static final int MSG_SIMPLEREQUEST_100_FIELD_CONNECTIONID_101 = 0x00800001;
    public static final int MSG_SIMPLEREQUEST_100_FIELD_PAYLOAD_103 = 0x01400003;
    
    public static final int MSG_SIMPLEDISCONNECT_101 = 0x00000004;
    public static final int MSG_SIMPLEDISCONNECT_101_FIELD_CONNECTIONID_101 = 0x00800001;
    
    public static final int MSG_ENCRYPTEDREQUEST_110 = 0x00000007;
    public static final int MSG_ENCRYPTEDREQUEST_110_FIELD_CONNECTIONID_101 = 0x00800001;
    public static final int MSG_ENCRYPTEDREQUEST_110_FIELD_ENCRYPTED_104 = 0x01c00003;
    
    protected ClientNetRequestSchema() {
        super(FROM);
    }
        
}
