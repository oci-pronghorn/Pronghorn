package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class MQTTClientToServerSchema extends MessageSchema<MQTTClientToServerSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0xa8000000,0x80000000,0xc0200003,0xc0400009,0x90000000,0x80000001,0x80000002,0xa8000001,0xa8000002,0xb8000003,0xa8000004,0xa8000005,0xc0200009,0xc0400002,0x90000000,0xc0200002,0xc0400007,0x90000000,0x80000003,0x80000004,0x80000005,0xa8000006,0xb8000007,0xc0200007,0xc0400005,0x90000000,0x80000003,0x80000004,0xa8000006,0xc0200005,0xc0400004,0x90000000,0x80000003,0xa8000006,0xc0200004,0xc0400003,0x90000000,0x80000003,0xc0200003},
		    (short)0,
		    new String[]{"BrokerHost","Host","Port",null,"Connect","Time","KeepAliveSec","Flags","ClientId",
		    "WillTopic","WillPayload","User","Pass",null,"Disconnect","Time",null,"Publish","Time",
		    "PacketId","QOS","Retain","Topic","Payload",null,"Subscribe","Time","PacketId","QOS",
		    "Topic",null,"UnSubscribe","Time","PacketId","Topic",null,"PubRec","Time","PacketId",
		    null},
		    new long[]{100, 26, 27, 0, 1, 37, 28, 29, 30, 31, 32, 33, 34, 0, 14, 37, 0, 3, 37, 20, 21, 22, 23, 25, 0, 8, 37, 20, 21, 23, 0, 10, 37, 20, 23, 0, 5, 37, 20, 0},
		    new String[]{"global",null,null,null,"global",null,null,null,null,null,null,null,null,null,"global",
		    null,null,"global",null,null,null,null,null,null,null,"global",null,null,null,null,
		    null,"global",null,null,null,null,"global",null,null,null},
		    "MQTTClientToServer.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    
    protected MQTTClientToServerSchema() {
        super(FROM);
    }

    
    public static final MQTTClientToServerSchema instance = new MQTTClientToServerSchema();
    
    public static final int MSG_BROKERHOST_100 = 0x00000000; //Group/OpenTempl/3
    public static final int MSG_BROKERHOST_100_FIELD_HOST_26 = 0x01400001; //UTF8/None/0
    public static final int MSG_BROKERHOST_100_FIELD_PORT_27 = 0x00000003; //IntegerUnsigned/None/0
    public static final int MSG_CONNECT_1 = 0x00000004; //Group/OpenTempl/9
    public static final int MSG_CONNECT_1_FIELD_TIME_37 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_CONNECT_1_FIELD_KEEPALIVESEC_28 = 0x00000003; //IntegerUnsigned/None/1
    public static final int MSG_CONNECT_1_FIELD_FLAGS_29 = 0x00000004; //IntegerUnsigned/None/2
    public static final int MSG_CONNECT_1_FIELD_CLIENTID_30 = 0x01400005; //UTF8/None/1
    public static final int MSG_CONNECT_1_FIELD_WILLTOPIC_31 = 0x01400007; //UTF8/None/2
    public static final int MSG_CONNECT_1_FIELD_WILLPAYLOAD_32 = 0x01c00009; //ByteVector/None/3
    public static final int MSG_CONNECT_1_FIELD_USER_33 = 0x0140000b; //UTF8/None/4
    public static final int MSG_CONNECT_1_FIELD_PASS_34 = 0x0140000d; //UTF8/None/5
    public static final int MSG_DISCONNECT_14 = 0x0000000e; //Group/OpenTempl/2
    public static final int MSG_DISCONNECT_14_FIELD_TIME_37 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_PUBLISH_3 = 0x00000011; //Group/OpenTempl/7
    public static final int MSG_PUBLISH_3_FIELD_TIME_37 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_PUBLISH_3_FIELD_PACKETID_20 = 0x00000003; //IntegerUnsigned/None/3
    public static final int MSG_PUBLISH_3_FIELD_QOS_21 = 0x00000004; //IntegerUnsigned/None/4
    public static final int MSG_PUBLISH_3_FIELD_RETAIN_22 = 0x00000005; //IntegerUnsigned/None/5
    public static final int MSG_PUBLISH_3_FIELD_TOPIC_23 = 0x01400006; //UTF8/None/6
    public static final int MSG_PUBLISH_3_FIELD_PAYLOAD_25 = 0x01c00008; //ByteVector/None/7
    public static final int MSG_SUBSCRIBE_8 = 0x00000019; //Group/OpenTempl/5
    public static final int MSG_SUBSCRIBE_8_FIELD_TIME_37 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_SUBSCRIBE_8_FIELD_PACKETID_20 = 0x00000003; //IntegerUnsigned/None/3
    public static final int MSG_SUBSCRIBE_8_FIELD_QOS_21 = 0x00000004; //IntegerUnsigned/None/4
    public static final int MSG_SUBSCRIBE_8_FIELD_TOPIC_23 = 0x01400005; //UTF8/None/6
    public static final int MSG_UNSUBSCRIBE_10 = 0x0000001f; //Group/OpenTempl/4
    public static final int MSG_UNSUBSCRIBE_10_FIELD_TIME_37 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_UNSUBSCRIBE_10_FIELD_PACKETID_20 = 0x00000003; //IntegerUnsigned/None/3
    public static final int MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23 = 0x01400004; //UTF8/None/6
    public static final int MSG_PUBREC_5 = 0x00000024; //Group/OpenTempl/3
    public static final int MSG_PUBREC_5_FIELD_TIME_37 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_PUBREC_5_FIELD_PACKETID_20 = 0x00000003; //IntegerUnsigned/None/3


    public static void consume(Pipe<MQTTClientToServerSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_BROKERHOST_100:
                    consumeBrokerHost(input);
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
                case MSG_SUBSCRIBE_8:
                    consumeSubscribe(input);
                break;
                case MSG_UNSUBSCRIBE_10:
                    consumeUnSubscribe(input);
                break;
                case MSG_PUBREC_5:
                    consumePubRec(input);
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
    public static void consumeConnect(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_CONNECT_1_FIELD_TIME_37);
        int fieldKeepAliveSec = PipeReader.readInt(input,MSG_CONNECT_1_FIELD_KEEPALIVESEC_28);
        int fieldFlags = PipeReader.readInt(input,MSG_CONNECT_1_FIELD_FLAGS_29);
        StringBuilder fieldClientId = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_CLIENTID_30,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_CLIENTID_30)));
        StringBuilder fieldWillTopic = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_WILLTOPIC_31,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_WILLTOPIC_31)));
        DataInputBlobReader<MQTTClientToServerSchema> fieldWillPayload = PipeReader.inputStream(input, MSG_CONNECT_1_FIELD_WILLPAYLOAD_32);
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
        DataInputBlobReader<MQTTClientToServerSchema> fieldPayload = PipeReader.inputStream(input, MSG_PUBLISH_3_FIELD_PAYLOAD_25);
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
    public static void consumePubRec(Pipe<MQTTClientToServerSchema> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBREC_5_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBREC_5_FIELD_PACKETID_20);
    }

    public static void publishBrokerHost(Pipe<MQTTClientToServerSchema> output, CharSequence fieldHost, int fieldPort) {
            PipeWriter.presumeWriteFragment(output, MSG_BROKERHOST_100);
            PipeWriter.writeUTF8(output,MSG_BROKERHOST_100_FIELD_HOST_26, fieldHost);
            PipeWriter.writeInt(output,MSG_BROKERHOST_100_FIELD_PORT_27, fieldPort);
            PipeWriter.publishWrites(output);
    }
    public static void publishConnect(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldKeepAliveSec, int fieldFlags, CharSequence fieldClientId, CharSequence fieldWillTopic, byte[] fieldWillPayloadBacking, int fieldWillPayloadPosition, int fieldWillPayloadLength, CharSequence fieldUser, CharSequence fieldPass) {
            PipeWriter.presumeWriteFragment(output, MSG_CONNECT_1);
            PipeWriter.writeLong(output,MSG_CONNECT_1_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_CONNECT_1_FIELD_KEEPALIVESEC_28, fieldKeepAliveSec);
            PipeWriter.writeInt(output,MSG_CONNECT_1_FIELD_FLAGS_29, fieldFlags);
            PipeWriter.writeUTF8(output,MSG_CONNECT_1_FIELD_CLIENTID_30, fieldClientId);
            PipeWriter.writeUTF8(output,MSG_CONNECT_1_FIELD_WILLTOPIC_31, fieldWillTopic);
            PipeWriter.writeBytes(output,MSG_CONNECT_1_FIELD_WILLPAYLOAD_32, fieldWillPayloadBacking, fieldWillPayloadPosition, fieldWillPayloadLength);
            PipeWriter.writeUTF8(output,MSG_CONNECT_1_FIELD_USER_33, fieldUser);
            PipeWriter.writeUTF8(output,MSG_CONNECT_1_FIELD_PASS_34, fieldPass);
            PipeWriter.publishWrites(output);
    }
    public static void publishDisconnect(Pipe<MQTTClientToServerSchema> output, long fieldTime) {
            PipeWriter.presumeWriteFragment(output, MSG_DISCONNECT_14);
            PipeWriter.writeLong(output,MSG_DISCONNECT_14_FIELD_TIME_37, fieldTime);
            PipeWriter.publishWrites(output);
    }
    public static void publishPublish(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId, int fieldQOS, int fieldRetain, CharSequence fieldTopic, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
            PipeWriter.presumeWriteFragment(output, MSG_PUBLISH_3);
            PipeWriter.writeLong(output,MSG_PUBLISH_3_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_QOS_21, fieldQOS);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_RETAIN_22, fieldRetain);
            PipeWriter.writeUTF8(output,MSG_PUBLISH_3_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.writeBytes(output,MSG_PUBLISH_3_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
    }
    public static void publishSubscribe(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId, int fieldQOS, CharSequence fieldTopic) {
            PipeWriter.presumeWriteFragment(output, MSG_SUBSCRIBE_8);
            PipeWriter.writeLong(output,MSG_SUBSCRIBE_8_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_SUBSCRIBE_8_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.writeInt(output,MSG_SUBSCRIBE_8_FIELD_QOS_21, fieldQOS);
            PipeWriter.writeUTF8(output,MSG_SUBSCRIBE_8_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.publishWrites(output);
    }
    public static void publishUnSubscribe(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId, CharSequence fieldTopic) {
            PipeWriter.presumeWriteFragment(output, MSG_UNSUBSCRIBE_10);
            PipeWriter.writeLong(output,MSG_UNSUBSCRIBE_10_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_UNSUBSCRIBE_10_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.writeUTF8(output,MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.publishWrites(output);
    }
    public static void publishPubRec(Pipe<MQTTClientToServerSchema> output, long fieldTime, int fieldPacketId) {
            PipeWriter.presumeWriteFragment(output, MSG_PUBREC_5);
            PipeWriter.writeLong(output,MSG_PUBREC_5_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBREC_5_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
    }
}
