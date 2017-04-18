package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class MQTTConnectionInSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400004,0x80000000,0x80000001,0xb8000000,0xc0200004,0xc0400003,0xa0000001,0xb8000000,0xc0200003,0xc0400001,0xc0200001,0xc0400003,0x80000001,0xb8000000,0xc0200003,0xc0400002,0xb8000000,0xc0200002,0xc0400002,0xb8000000,0xc0200002,0xc0400003,0x80000001,0xb8000000,0xc0200003,0xc0400003,0x80000001,0xb8000000,0xc0200003,0xc0400003,0x80000001,0xb8000000,0xc0200003},
            (short)0,
            new String[]{"Publish","QOS","PacketId","PacketData",null,"Connect","URL","PacketData",null,"Disconnect",
            null,"PubRel","PacketId","PacketData",null,"Subscribe","PacketData",null,"UnSubscribe",
            "PacketData",null,"PubAck","PacketId","PacketData",null,"PubRec","PacketId","PacketData",
            null,"PubComp","PacketId","PacketData",null},
            new long[]{1, 100, 200, 300, 0, 2, 400, 300, 0, 5, 0, 9, 200, 300, 0, 3, 300, 0, 4, 300, 0, 6, 200, 300, 0, 7, 200, 300, 0, 8, 200, 300, 0},
            new String[]{"global",null,null,null,null,"global",null,null,null,"global",null,"global",null,
            null,null,"global",null,null,"global",null,null,"global",null,null,null,"global",
            null,null,null,"global",null,null,null},
            "MQTTConnectionIn.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});

    
    public static final MQTTConnectionInSchema instance = new MQTTConnectionInSchema();
    
    public static final int MSG_PUBLISH_1 = 0x00000000;
    public static final int MSG_PUBLISH_1_FIELD_QOS_100 = 0x00000001;
    public static final int MSG_PUBLISH_1_FIELD_PACKETID_200 = 0x00000002;
    public static final int MSG_PUBLISH_1_FIELD_PACKETDATA_300 = 0x01c00003;
    public static final int MSG_CONNECT_2 = 0x00000005;
    public static final int MSG_CONNECT_2_FIELD_URL_400 = 0x01000001;
    public static final int MSG_CONNECT_2_FIELD_PACKETDATA_300 = 0x01c00003;
    public static final int MSG_DISCONNECT_5 = 0x00000009;
    public static final int MSG_PUBREL_9 = 0x0000000b;
    public static final int MSG_PUBREL_9_FIELD_PACKETID_200 = 0x00000001;
    public static final int MSG_PUBREL_9_FIELD_PACKETDATA_300 = 0x01c00002;
    public static final int MSG_SUBSCRIBE_3 = 0x0000000f;
    public static final int MSG_SUBSCRIBE_3_FIELD_PACKETDATA_300 = 0x01c00001;
    public static final int MSG_UNSUBSCRIBE_4 = 0x00000012;
    public static final int MSG_UNSUBSCRIBE_4_FIELD_PACKETDATA_300 = 0x01c00001;
    public static final int MSG_PUBACK_6 = 0x00000015;
    public static final int MSG_PUBACK_6_FIELD_PACKETID_200 = 0x00000001;
    public static final int MSG_PUBACK_6_FIELD_PACKETDATA_300 = 0x01c00002;
    public static final int MSG_PUBREC_7 = 0x00000019;
    public static final int MSG_PUBREC_7_FIELD_PACKETID_200 = 0x00000001;
    public static final int MSG_PUBREC_7_FIELD_PACKETDATA_300 = 0x01c00002;
    public static final int MSG_PUBCOMP_8 = 0x0000001d;
    public static final int MSG_PUBCOMP_8_FIELD_PACKETID_200 = 0x00000001;
    public static final int MSG_PUBCOMP_8_FIELD_PACKETDATA_300 = 0x01c00002;

    protected MQTTConnectionInSchema() {
        super(FROM);
    }
        
}
