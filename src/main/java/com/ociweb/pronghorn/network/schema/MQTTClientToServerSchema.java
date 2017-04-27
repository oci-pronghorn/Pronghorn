package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class MQTTClientToServerSchema extends MessageSchema<MQTTClientToServerSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0xa8000000,0x80000000,0xc0200003,0xc0400002,0x80000001,0xc0200002,0xc0400009,0x90000000,0x80000002,0x80000003,0xa8000001,0xa8000002,0xb8000003,0xa8000004,0xa8000005,0xc0200009,0xc0400002,0x90000000,0xc0200002,0xc0400007,0x90000000,0x80000001,0x80000004,0x80000005,0xa8000006,0xb8000007,0xc0200007,0xc0400003,0x90000000,0x80000001,0xc0200003,0xc0400005,0x90000000,0x80000001,0x80000004,0xa8000006,0xc0200005,0xc0400004,0x90000000,0x80000001,0xa8000006,0xc0200004,0xc0400003,0x90000000,0x80000001,0xc0200003,0xc0400003,0x90000000,0x80000001,0xc0200003,0xc0400003,0x90000000,0x80000001,0xc0200003},
		    (short)0,
		    new String[]{"BrokerHost","Host","Port",null,"StopRePublish","PacketId",null,"Connect","Time",
		    "KeepAliveSec","Flags","ClientId","WillTopic","WillPayload","User","Pass",null,"Disconnect",
		    "Time",null,"Publish","Time","PacketId","QOS","Retain","Topic","Payload",null,"PubRel",
		    "Time","PacketId",null,"Subscribe","Time","PacketId","QOS","Topic",null,"UnSubscribe",
		    "Time","PacketId","Topic",null,"PubAck","Time","PacketId",null,"PubRec","Time","PacketId",
		    null,"PubComp","Time","PacketId",null},
		    new long[]{100, 26, 27, 0, 99, 20, 0, 1, 37, 28, 29, 30, 31, 32, 33, 34, 0, 14, 37, 0, 3, 37, 20, 21, 22, 23, 25, 0, 6, 37, 20, 0, 8, 37, 20, 21, 23, 0, 10, 37, 20, 23, 0, 4, 37, 20, 0, 5, 37, 20, 0, 7, 37, 20, 0},
		    new String[]{"global",null,null,null,"global",null,null,"global",null,null,null,null,null,null,
		    null,null,null,"global",null,null,"global",null,null,null,null,null,null,null,"global",
		    null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,"global",
		    null,null,null,"global",null,null,null,"global",null,null,null},
		    "MQTTClientToServer.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    
    protected MQTTClientToServerSchema() {
        super(FROM);
    }

    
    public static final MQTTClientToServerSchema instance = new MQTTClientToServerSchema();
    
    public static final int MSG_BROKERHOST_100 = 0x00000000;
    public static final int MSG_BROKERHOST_100_FIELD_HOST_26 = 0x01400001;
    public static final int MSG_BROKERHOST_100_FIELD_PORT_27 = 0x00000003;
    public static final int MSG_STOPREPUBLISH_99 = 0x00000004;
    public static final int MSG_STOPREPUBLISH_99_FIELD_PACKETID_20 = 0x00000001;
    public static final int MSG_CONNECT_1 = 0x00000007;
    public static final int MSG_CONNECT_1_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_CONNECT_1_FIELD_KEEPALIVESEC_28 = 0x00000003;
    public static final int MSG_CONNECT_1_FIELD_FLAGS_29 = 0x00000004;
    public static final int MSG_CONNECT_1_FIELD_CLIENTID_30 = 0x01400005;
    public static final int MSG_CONNECT_1_FIELD_WILLTOPIC_31 = 0x01400007;
    public static final int MSG_CONNECT_1_FIELD_WILLPAYLOAD_32 = 0x01c00009;
    public static final int MSG_CONNECT_1_FIELD_USER_33 = 0x0140000b;
    public static final int MSG_CONNECT_1_FIELD_PASS_34 = 0x0140000d;
    public static final int MSG_DISCONNECT_14 = 0x00000011;
    public static final int MSG_DISCONNECT_14_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBLISH_3 = 0x00000014;
    public static final int MSG_PUBLISH_3_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBLISH_3_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_PUBLISH_3_FIELD_QOS_21 = 0x00000004;
    public static final int MSG_PUBLISH_3_FIELD_RETAIN_22 = 0x00000005;
    public static final int MSG_PUBLISH_3_FIELD_TOPIC_23 = 0x01400006;
    public static final int MSG_PUBLISH_3_FIELD_PAYLOAD_25 = 0x01c00008;
    public static final int MSG_PUBREL_6 = 0x0000001c;
    public static final int MSG_PUBREL_6_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBREL_6_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_SUBSCRIBE_8 = 0x00000020;
    public static final int MSG_SUBSCRIBE_8_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_SUBSCRIBE_8_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_SUBSCRIBE_8_FIELD_QOS_21 = 0x00000004;
    public static final int MSG_SUBSCRIBE_8_FIELD_TOPIC_23 = 0x01400005;
    public static final int MSG_UNSUBSCRIBE_10 = 0x00000026;
    public static final int MSG_UNSUBSCRIBE_10_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_UNSUBSCRIBE_10_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23 = 0x01400004;
    public static final int MSG_PUBACK_4 = 0x0000002b;
    public static final int MSG_PUBACK_4_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBACK_4_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_PUBREC_5 = 0x0000002f;
    public static final int MSG_PUBREC_5_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBREC_5_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_PUBCOMP_7 = 0x00000033;
    public static final int MSG_PUBCOMP_7_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBCOMP_7_FIELD_PACKETID_20 = 0x00000003;


    public static void consume(Pipe<MQTTClientToServerSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_BROKERHOST_100:
                    consumeBrokerHost(input);
                break;
                case MSG_STOPREPUBLISH_99:
                    consumeStopRePublish(input);
                break;
                case MSG_CONNECT_1:
                    consumeConnect(input);
                break;
                case MSG_DISCONNECT_14:
                    consumeDisconnect(input);
                break;
                case MSG_PUBLISH_3:
                    consumePublish(input);
                break;
                case MSG_PUBREL_6:
                    consumePubRel(input);
                break;
                case MSG_SUBSCRIBE_8:
                    consumeSubscribe(input);
                break;
                case MSG_UNSUBSCRIBE_10:
                    consumeUnSubscribe(input);
                break;
                case MSG_PUBACK_4:
                    consumePubAck(input);
                break;
                case MSG_PUBREC_5:
                    consumePubRec(input);
                break;
                case MSG_PUBCOMP_7:
                    consumePubComp(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeBrokerHost(Pipe<MQTTClientToServerSchema> input) {
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_BROKERHOST_100_FIELD_HOST_26,new StringBuilder(PipeReader.readBytesLength(input,MSG_BROKERHOST_100_FIELD_HOST_26)));
        int fieldPort = PipeReader.readInt(input,MSG_BROKERHOST_100_FIELD_PORT_27);
    }
    public static void consumeStopRePublish(Pipe<MQTTClientToServerSchema> input) {
        int fieldPacketId = PipeReader.readInt(input,MSG_STOPREPUBLISH_99_FIELD_PACKETID_20);
    }
    public static void consumeConnect(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_CONNECT_1_FIELD_TIME_37);
        int fieldKeepAliveSec = PipeReader.readInt(input,MSG_CONNECT_1_FIELD_KEEPALIVESEC_28);
        int fieldFlags = PipeReader.readInt(input,MSG_CONNECT_1_FIELD_FLAGS_29);
        StringBuilder fieldClientId = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_CLIENTID_30,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_CLIENTID_30)));
        StringBuilder fieldWillTopic = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_WILLTOPIC_31,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_WILLTOPIC_31)));
        ByteBuffer fieldWillPayload = PipeReader.readBytes(input,MSG_CONNECT_1_FIELD_WILLPAYLOAD_32,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_WILLPAYLOAD_32)));
        StringBuilder fieldUser = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_USER_33,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_USER_33)));
        StringBuilder fieldPass = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_PASS_34,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_PASS_34)));
    }
    public static void consumeDisconnect(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_DISCONNECT_14_FIELD_TIME_37);
    }
    public static void consumePublish(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBLISH_3_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBLISH_3_FIELD_PACKETID_20);
        int fieldQOS = PipeReader.readInt(input,MSG_PUBLISH_3_FIELD_QOS_21);
        int fieldRetain = PipeReader.readInt(input,MSG_PUBLISH_3_FIELD_RETAIN_22);
        StringBuilder fieldTopic = PipeReader.readUTF8(input,MSG_PUBLISH_3_FIELD_TOPIC_23,new StringBuilder(PipeReader.readBytesLength(input,MSG_PUBLISH_3_FIELD_TOPIC_23)));
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_PUBLISH_3_FIELD_PAYLOAD_25,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_PUBLISH_3_FIELD_PAYLOAD_25)));
    }
    public static void consumePubRel(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBREL_6_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBREL_6_FIELD_PACKETID_20);
    }
    public static void consumeSubscribe(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_SUBSCRIBE_8_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_SUBSCRIBE_8_FIELD_PACKETID_20);
        int fieldQOS = PipeReader.readInt(input,MSG_SUBSCRIBE_8_FIELD_QOS_21);
        StringBuilder fieldTopic = PipeReader.readUTF8(input,MSG_SUBSCRIBE_8_FIELD_TOPIC_23,new StringBuilder(PipeReader.readBytesLength(input,MSG_SUBSCRIBE_8_FIELD_TOPIC_23)));
    }
    public static void consumeUnSubscribe(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_UNSUBSCRIBE_10_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_UNSUBSCRIBE_10_FIELD_PACKETID_20);
        StringBuilder fieldTopic = PipeReader.readUTF8(input,MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23,new StringBuilder(PipeReader.readBytesLength(input,MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23)));
    }
    public static void consumePubAck(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBACK_4_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBACK_4_FIELD_PACKETID_20);
    }
    public static void consumePubRec(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBREC_5_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBREC_5_FIELD_PACKETID_20);
    }
    public static void consumePubComp(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBCOMP_7_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBCOMP_7_FIELD_PACKETID_20);
    }

    public static boolean publishBrokerHost(Pipe<MQTTClientToServerSchema> output, CharSequence fieldHost, int fieldPort) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_BROKERHOST_100)) {
            PipeWriter.writeUTF8(output,MSG_BROKERHOST_100_FIELD_HOST_26, fieldHost);
            PipeWriter.writeInt(output,MSG_BROKERHOST_100_FIELD_PORT_27, fieldPort);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishStopRePublish(Pipe<MQTTClientToServerSchema> output, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_STOPREPUBLISH_99)) {
            PipeWriter.writeInt(output,MSG_STOPREPUBLISH_99_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishConnect(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldKeepAliveSec, int fieldFlags, CharSequence fieldClientId, CharSequence fieldWillTopic, byte[] fieldWillPayloadBacking, int fieldWillPayloadPosition, int fieldWillPayloadLength, CharSequence fieldUser, CharSequence fieldPass) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_CONNECT_1)) {
            PipeWriter.writeLong(output,MSG_CONNECT_1_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_CONNECT_1_FIELD_KEEPALIVESEC_28, fieldKeepAliveSec);
            PipeWriter.writeInt(output,MSG_CONNECT_1_FIELD_FLAGS_29, fieldFlags);
            PipeWriter.writeUTF8(output,MSG_CONNECT_1_FIELD_CLIENTID_30, fieldClientId);
            PipeWriter.writeUTF8(output,MSG_CONNECT_1_FIELD_WILLTOPIC_31, fieldWillTopic);
            PipeWriter.writeBytes(output,MSG_CONNECT_1_FIELD_WILLPAYLOAD_32, fieldWillPayloadBacking, fieldWillPayloadPosition, fieldWillPayloadLength);
            PipeWriter.writeUTF8(output,MSG_CONNECT_1_FIELD_USER_33, fieldUser);
            PipeWriter.writeUTF8(output,MSG_CONNECT_1_FIELD_PASS_34, fieldPass);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishDisconnect(Pipe<MQTTClientToServerSchema> output, long fieldTime) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_DISCONNECT_14)) {
            PipeWriter.writeLong(output,MSG_DISCONNECT_14_FIELD_TIME_37, fieldTime);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPublish(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId, int fieldQOS, int fieldRetain, CharSequence fieldTopic, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBLISH_3)) {
            PipeWriter.writeLong(output,MSG_PUBLISH_3_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_QOS_21, fieldQOS);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_RETAIN_22, fieldRetain);
            PipeWriter.writeUTF8(output,MSG_PUBLISH_3_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.writeBytes(output,MSG_PUBLISH_3_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPubRel(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBREL_6)) {
            PipeWriter.writeLong(output,MSG_PUBREL_6_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBREL_6_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishSubscribe(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId, int fieldQOS, CharSequence fieldTopic) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_SUBSCRIBE_8)) {
            PipeWriter.writeLong(output,MSG_SUBSCRIBE_8_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_SUBSCRIBE_8_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.writeInt(output,MSG_SUBSCRIBE_8_FIELD_QOS_21, fieldQOS);
            PipeWriter.writeUTF8(output,MSG_SUBSCRIBE_8_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishUnSubscribe(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId, CharSequence fieldTopic) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_UNSUBSCRIBE_10)) {
            PipeWriter.writeLong(output,MSG_UNSUBSCRIBE_10_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_UNSUBSCRIBE_10_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.writeUTF8(output,MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPubAck(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBACK_4)) {
            PipeWriter.writeLong(output,MSG_PUBACK_4_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBACK_4_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPubRec(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBREC_5)) {
            PipeWriter.writeLong(output,MSG_PUBREC_5_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBREC_5_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPubComp(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBCOMP_7)) {
            PipeWriter.writeLong(output,MSG_PUBCOMP_7_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBCOMP_7_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }

        
}
