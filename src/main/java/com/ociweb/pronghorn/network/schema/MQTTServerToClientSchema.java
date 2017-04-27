package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class MQTTServerToClientSchema extends MessageSchema<MQTTServerToClientSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0x80000000,0xc0200003,0xc0400003,0x90000000,0x80000000,0xc0200003,0xc0400003,0x90000000,0x80000000,0xc0200003,0xc0400004,0x90000000,0x80000001,0x80000002,0xc0200004,0xc0400008,0x90000000,0x80000000,0x80000003,0x80000004,0x80000005,0xa8000000,0xb8000001,0xc0200008,0xc0400002,0x90000000,0xc0200002,0xc0400003,0x90000000,0x80000000,0xc0200003,0xc0400004,0x90000000,0x80000000,0x80000002,0xc0200004,0xc0400003,0x90000000,0x80000000,0xc0200003,0xc0400002,0x90000000,0xc0200002},
		    (short)0,
		    new String[]{"PubAck","Time","PacketId",null,"PubRec","Time","PacketId",null,"PubRel","Time","PacketId",
		    null,"ConnAck","Time","Flag","ReturnCode",null,"Publish","Time","PacketId","QOS",
		    "Retain","Dup","Topic","Payload",null,"Disconnect","Time",null,"PubComp","Time","PacketId",
		    null,"SubAck","Time","PacketId","ReturnCode",null,"UnsubAck","Time","PacketId",null,
		    "PingResp","Time",null},
		    new long[]{4, 37, 20, 0, 5, 37, 20, 0, 6, 37, 20, 0, 2, 37, 35, 24, 0, 3, 37, 20, 21, 22, 36, 23, 25, 0, 14, 37, 0, 7, 37, 20, 0, 9, 37, 20, 24, 0, 11, 37, 20, 0, 13, 37, 0},
		    new String[]{"global",null,null,null,"global",null,null,null,"global",null,null,null,"global",
		    null,null,null,null,"global",null,null,null,null,null,null,null,null,"global",null,
		    null,"global",null,null,null,"global",null,null,null,null,"global",null,null,null,
		    "global",null,null},
		    "MQTTServerToClient.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});

    
    protected MQTTServerToClientSchema() {
        super(FROM);
    }
       
    
    public static final MQTTServerToClientSchema instance = new MQTTServerToClientSchema();
    
    public static final int MSG_PUBACK_4 = 0x00000000;
    public static final int MSG_PUBACK_4_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBACK_4_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_PUBREC_5 = 0x00000004;
    public static final int MSG_PUBREC_5_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBREC_5_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_PUBREL_6 = 0x00000008;
    public static final int MSG_PUBREL_6_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBREL_6_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_CONNACK_2 = 0x0000000c;
    public static final int MSG_CONNACK_2_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_CONNACK_2_FIELD_FLAG_35 = 0x00000003;
    public static final int MSG_CONNACK_2_FIELD_RETURNCODE_24 = 0x00000004;
    public static final int MSG_PUBLISH_3 = 0x00000011;
    public static final int MSG_PUBLISH_3_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBLISH_3_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_PUBLISH_3_FIELD_QOS_21 = 0x00000004;
    public static final int MSG_PUBLISH_3_FIELD_RETAIN_22 = 0x00000005;
    public static final int MSG_PUBLISH_3_FIELD_DUP_36 = 0x00000006;
    public static final int MSG_PUBLISH_3_FIELD_TOPIC_23 = 0x01400007;
    public static final int MSG_PUBLISH_3_FIELD_PAYLOAD_25 = 0x01c00009;
    public static final int MSG_DISCONNECT_14 = 0x0000001a;
    public static final int MSG_DISCONNECT_14_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBCOMP_7 = 0x0000001d;
    public static final int MSG_PUBCOMP_7_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_PUBCOMP_7_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_SUBACK_9 = 0x00000021;
    public static final int MSG_SUBACK_9_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_SUBACK_9_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_SUBACK_9_FIELD_RETURNCODE_24 = 0x00000004;
    public static final int MSG_UNSUBACK_11 = 0x00000026;
    public static final int MSG_UNSUBACK_11_FIELD_TIME_37 = 0x00800001;
    public static final int MSG_UNSUBACK_11_FIELD_PACKETID_20 = 0x00000003;
    public static final int MSG_PINGRESP_13 = 0x0000002a;
    public static final int MSG_PINGRESP_13_FIELD_TIME_37 = 0x00800001;


    public static void consume(Pipe<MQTTServerToClientSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_PUBACK_4:
                    consumePubAck(input);
                break;
                case MSG_PUBREC_5:
                    consumePubRec(input);
                break;
                case MSG_PUBREL_6:
                    consumePubRel(input);
                break;
                case MSG_CONNACK_2:
                    consumeConnAck(input);
                break;
                case MSG_PUBLISH_3:
                    consumePublish(input);
                break;
                case MSG_DISCONNECT_14:
                    consumeDisconnect(input);
                break;
                case MSG_PUBCOMP_7:
                    consumePubComp(input);
                break;
                case MSG_SUBACK_9:
                    consumeSubAck(input);
                break;
                case MSG_UNSUBACK_11:
                    consumeUnsubAck(input);
                break;
                case MSG_PINGRESP_13:
                    consumePingResp(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumePubAck(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBACK_4_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBACK_4_FIELD_PACKETID_20);
    }
    public static void consumePubRec(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBREC_5_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBREC_5_FIELD_PACKETID_20);
    }
    public static void consumePubRel(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBREL_6_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBREL_6_FIELD_PACKETID_20);
    }
    public static void consumeConnAck(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_CONNACK_2_FIELD_TIME_37);
        int fieldFlag = PipeReader.readInt(input,MSG_CONNACK_2_FIELD_FLAG_35);
        int fieldReturnCode = PipeReader.readInt(input,MSG_CONNACK_2_FIELD_RETURNCODE_24);
    }
    public static void consumePublish(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBLISH_3_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBLISH_3_FIELD_PACKETID_20);
        int fieldQOS = PipeReader.readInt(input,MSG_PUBLISH_3_FIELD_QOS_21);
        int fieldRetain = PipeReader.readInt(input,MSG_PUBLISH_3_FIELD_RETAIN_22);
        int fieldDup = PipeReader.readInt(input,MSG_PUBLISH_3_FIELD_DUP_36);
        StringBuilder fieldTopic = PipeReader.readUTF8(input,MSG_PUBLISH_3_FIELD_TOPIC_23,new StringBuilder(PipeReader.readBytesLength(input,MSG_PUBLISH_3_FIELD_TOPIC_23)));
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_PUBLISH_3_FIELD_PAYLOAD_25,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_PUBLISH_3_FIELD_PAYLOAD_25)));
    }
    public static void consumeDisconnect(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_DISCONNECT_14_FIELD_TIME_37);
    }
    public static void consumePubComp(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBCOMP_7_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBCOMP_7_FIELD_PACKETID_20);
    }
    public static void consumeSubAck(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_SUBACK_9_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_SUBACK_9_FIELD_PACKETID_20);
        int fieldReturnCode = PipeReader.readInt(input,MSG_SUBACK_9_FIELD_RETURNCODE_24);
    }
    public static void consumeUnsubAck(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_UNSUBACK_11_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_UNSUBACK_11_FIELD_PACKETID_20);
    }
    public static void consumePingResp(Pipe<MQTTServerToClientSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PINGRESP_13_FIELD_TIME_37);
    }

    public static boolean publishPubAck(Pipe<MQTTServerToClientSchema> output, long fieldTime, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBACK_4)) {
            PipeWriter.writeLong(output,MSG_PUBACK_4_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBACK_4_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPubRec(Pipe<MQTTServerToClientSchema> output, long fieldTime, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBREC_5)) {
            PipeWriter.writeLong(output,MSG_PUBREC_5_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBREC_5_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPubRel(Pipe<MQTTServerToClientSchema> output, long fieldTime, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBREL_6)) {
            PipeWriter.writeLong(output,MSG_PUBREL_6_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBREL_6_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishConnAck(Pipe<MQTTServerToClientSchema> output, long fieldTime, int fieldFlag, int fieldReturnCode) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_CONNACK_2)) {
            PipeWriter.writeLong(output,MSG_CONNACK_2_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_CONNACK_2_FIELD_FLAG_35, fieldFlag);
            PipeWriter.writeInt(output,MSG_CONNACK_2_FIELD_RETURNCODE_24, fieldReturnCode);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPublish(Pipe<MQTTServerToClientSchema> output, long fieldTime, int fieldPacketId, int fieldQOS, int fieldRetain, int fieldDup, CharSequence fieldTopic, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBLISH_3)) {
            PipeWriter.writeLong(output,MSG_PUBLISH_3_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_QOS_21, fieldQOS);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_RETAIN_22, fieldRetain);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_DUP_36, fieldDup);
            PipeWriter.writeUTF8(output,MSG_PUBLISH_3_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.writeBytes(output,MSG_PUBLISH_3_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishDisconnect(Pipe<MQTTServerToClientSchema> output, long fieldTime) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_DISCONNECT_14)) {
            PipeWriter.writeLong(output,MSG_DISCONNECT_14_FIELD_TIME_37, fieldTime);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPubComp(Pipe<MQTTServerToClientSchema> output, long fieldTime, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBCOMP_7)) {
            PipeWriter.writeLong(output,MSG_PUBCOMP_7_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBCOMP_7_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishSubAck(Pipe<MQTTServerToClientSchema> output, long fieldTime, int fieldPacketId, int fieldReturnCode) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_SUBACK_9)) {
            PipeWriter.writeLong(output,MSG_SUBACK_9_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_SUBACK_9_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.writeInt(output,MSG_SUBACK_9_FIELD_RETURNCODE_24, fieldReturnCode);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishUnsubAck(Pipe<MQTTServerToClientSchema> output, long fieldTime, int fieldPacketId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_UNSUBACK_11)) {
            PipeWriter.writeLong(output,MSG_UNSUBACK_11_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_UNSUBACK_11_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishPingResp(Pipe<MQTTServerToClientSchema> output, long fieldTime) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PINGRESP_13)) {
            PipeWriter.writeLong(output,MSG_PINGRESP_13_FIELD_TIME_37, fieldTime);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


 
}
