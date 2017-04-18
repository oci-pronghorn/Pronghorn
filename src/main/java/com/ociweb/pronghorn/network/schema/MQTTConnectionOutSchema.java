package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class MQTTConnectionOutSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400002,0x80000000,0xc0200002,0xc0400002,0x80000000,0xc0200002,0xc0400001,0xc0200001,0xc0400001,0xc0200001,0xc0400001,0xc0200001,0xc0400001,0xc0200001,0xc0400001,0xc0200001,0xc0400001,0xc0200001,0xc0400005,0x80000001,0x80000000,0xa0000000,0xa0000001,0xc0200005,0xc0400002,0x80000000,0xc0200002},
            (short)0,
            new String[]{"PubAck","PacketId",null,"PubRec","PacketId",null,"ConnAckOK",null,"ConnAckProto",
            null,"ConnAckId",null,"ConnAckServer",null,"ConnAckUser",null,"ConnAckAuth",null,
            "Message","QOS","PacketId","Topic","Payload",null,"PubRel","PacketId",null},
            new long[]{6, 200, 0, 7, 200, 0, 20, 0, 21, 0, 22, 0, 23, 0, 24, 0, 25, 0, 10, 100, 200, 400, 500, 0, 9, 200, 0},
            new String[]{"global",null,null,"global",null,null,"global",null,"global",null,"global",null,"global",
            null,"global",null,"global",null,"global",null,null,null,null,null,"global",null,
            null},
            "MQTTConnectionOut.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});

    
    public static final MQTTConnectionOutSchema instance = new MQTTConnectionOutSchema();
    
    public static final int MSG_PUBACK_6 = 0x00000000;
    public static final int MSG_PUBACK_6_FIELD_PACKETID_200 = 0x00000001;
    public static final int MSG_PUBREC_7 = 0x00000003;
    public static final int MSG_PUBREC_7_FIELD_PACKETID_200 = 0x00000001;
    public static final int MSG_CONNACKOK_20 = 0x00000006;
    public static final int MSG_CONNACKPROTO_21 = 0x00000008;
    public static final int MSG_CONNACKID_22 = 0x0000000a;
    public static final int MSG_CONNACKSERVER_23 = 0x0000000c;
    public static final int MSG_CONNACKUSER_24 = 0x0000000e;
    public static final int MSG_CONNACKAUTH_25 = 0x00000010;
    public static final int MSG_MESSAGE_10 = 0x00000012;
    public static final int MSG_MESSAGE_10_FIELD_QOS_100 = 0x00000001;
    public static final int MSG_MESSAGE_10_FIELD_PACKETID_200 = 0x00000002;
    public static final int MSG_MESSAGE_10_FIELD_TOPIC_400 = 0x01000003;
    public static final int MSG_MESSAGE_10_FIELD_PAYLOAD_500 = 0x01000005;
    public static final int MSG_PUBREL_9 = 0x00000018;
    public static final int MSG_PUBREL_9_FIELD_PACKETID_200 = 0x00000001;

    protected MQTTConnectionOutSchema() {
        super(FROM);
    }
        
}
