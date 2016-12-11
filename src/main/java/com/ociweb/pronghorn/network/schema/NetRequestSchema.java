package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class NetRequestSchema extends MessageSchema {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400005,0x80000000,0xa8000000,0xa8000001,0x80000001,0xc0200005,0xc0400006,0x80000000,0xa8000000,0xa8000001,0xb8000002,0x80000001,0xc0200006,0xc0400007,0x80000000,0xa8000000,0xa8000001,0x90000000,0xb8000002,0x80000001,0xc0200007,0xc0400003,0xb8000002,0x80000001,0xc0200003,0xc0400004,0x80000000,0xa8000000,0x80000001,0xc0200004},
		    (short)0,
		    new String[]{"HTTPGet","Port","Host","Path","Listener",null,"HTTPPost","Port","Host","Path","Payload","Listener",null,"HTTPPostChunked","Port","Host","Path","TotalLength","PayloadChunk","Listener",null,"HTTPPostChunk","PayloadChunk","Listener",null,"Close","Port","Host","Listener",null},
		    new long[]{100, 1, 2, 3, 10, 0, 101, 1, 2, 3, 5, 10, 0, 102, 1, 2, 3, 6, 5, 10, 0, 103, 5, 10, 0, 104, 1, 2, 10, 0},
		    new String[]{"global",null,null,null,null,null,"global",null,null,null,null,null,null,"global",null,null,null,null,null,null,null,"global",null,null,null,"global",null,null,null,null},
		    "NetRequest.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    
    public static final NetRequestSchema instance = new NetRequestSchema();
    
    public static final int MSG_HTTPGET_100 = 0x00000000;
    public static final int MSG_HTTPGET_100_FIELD_PORT_1 = 0x00000001;
    public static final int MSG_HTTPGET_100_FIELD_HOST_2 = 0x01400002;
    public static final int MSG_HTTPGET_100_FIELD_PATH_3 = 0x01400004;
    public static final int MSG_HTTPGET_100_FIELD_LISTENER_10 = 0x00000006;
    
    public static final int MSG_HTTPPOST_101 = 0x00000006;
    public static final int MSG_HTTPPOST_101_FIELD_PORT_1 = 0x00000001;
    public static final int MSG_HTTPPOST_101_FIELD_HOST_2 = 0x01400002;
    public static final int MSG_HTTPPOST_101_FIELD_PATH_3 = 0x01400004;
    public static final int MSG_HTTPPOST_101_FIELD_PAYLOAD_5 = 0x01c00006;
    public static final int MSG_HTTPPOST_101_FIELD_LISTENER_10 = 0x00000008;
    
    public static final int MSG_HTTPPOSTCHUNKED_102 = 0x0000000d;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_PORT_1 = 0x00000001;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_HOST_2 = 0x01400002;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_PATH_3 = 0x01400004;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_TOTALLENGTH_6 = 0x00800006;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_PAYLOADCHUNK_5 = 0x01c00008;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_LISTENER_10 = 0x0000000a;
    
    public static final int MSG_HTTPPOSTCHUNK_103 = 0x00000015;
    public static final int MSG_HTTPPOSTCHUNK_103_FIELD_PAYLOADCHUNK_5 = 0x01c00001;
    public static final int MSG_HTTPPOSTCHUNK_103_FIELD_LISTENER_10 = 0x00000003;
    
    public static final int MSG_CLOSE_104 = 0x00000019;
    public static final int MSG_CLOSE_104_FIELD_PORT_1 = 0x00000001;
    public static final int MSG_CLOSE_104_FIELD_HOST_2 = 0x01400002;
    public static final int MSG_CLOSE_104_FIELD_LISTENER_10 = 0x00000004;
    
    protected NetRequestSchema() {
        super(FROM);
    }
        
}
