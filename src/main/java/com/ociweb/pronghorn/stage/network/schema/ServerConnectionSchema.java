package com.ociweb.pronghorn.stage.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class ServerConnectionSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc1400003,0x90200000,0x90800001,0xc1200003},
            (short)0,
            new String[]{"ServerConnection","ConnectionGroup","ChannelId",null},
            new long[]{100, 1, 2, 0},
            new String[]{"global",null,null,null},
            "serverConnect.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    
    public static final ServerConnectionSchema instance = new ServerConnectionSchema(FROM);
    
    public static final int MSG_SERVERCONNECTION_100 = 0x00000000;
    public static final int MSG_SERVERCONNECTION_100_FIELD_CONNECTIONGROUP_1 = 0x00800001;
    public static final int MSG_SERVERCONNECTION_100_FIELD_CHANNELID_2 = 0x00800003;

    
    private ServerConnectionSchema(FieldReferenceOffsetManager from) {
        super(from);
    }

    
    
}
