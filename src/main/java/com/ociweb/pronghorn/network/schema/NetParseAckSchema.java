package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class NetParseAckSchema extends MessageSchema {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0x90000001,0xc0200003},
		    (short)0,
		    new String[]{"ParseAck","ConnectionID","Position",null},
		    new long[]{100, 1, 2, 0},
		    new String[]{"global",null,null,null},
		    "NetParseAck.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    
    public static final NetParseAckSchema instance = new NetParseAckSchema();
    
    public static final int MSG_PARSEACK_100 = 0x00000000;
    public static final int MSG_PARSEACK_100_FIELD_CONNECTIONID_1 = 0x00800001;
    public static final int MSG_PARSEACK_100_FIELD_POSITION_2 = 0x00800003;
    
    protected NetParseAckSchema() {
        super(FROM);
    }
        
}
