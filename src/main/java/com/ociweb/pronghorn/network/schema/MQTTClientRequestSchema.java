package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class MQTTClientRequestSchema extends MessageSchema<MQTTClientRequestSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0xa8000000,0x80000000,0xc0200003,0xc0400008,0x80000001,0x80000002,0xa8000001,0xa8000002,0xb8000003,0xa8000004,0xa8000005,0xc0200008,0xc0400005,0x80000003,0x80000004,0xa8000006,0xb8000007,0xc0200005,0xc0400003,0x80000003,0xa8000006,0xc0200003,0xc0400002,0xa8000006,0xc0200002},
		    (short)0,
		    new String[]{"BrokerConfig","Host","Port",null,"Connect","KeepAliveSec","Flags","ClientId","WillTopic",
		    "WillPayload","User","Pass",null,"Publish","QOS","Retain","Topic","Payload",null,
		    "Subscribe","QOS","Topic",null,"UnSubscribe","Topic",null},
		    new long[]{100, 26, 27, 0, 1, 28, 29, 30, 31, 32, 33, 34, 0, 3, 21, 22, 23, 25, 0, 8, 21, 23, 0, 10, 23, 0},
		    new String[]{"global",null,null,null,"global",null,null,null,null,null,null,null,null,"global",
		    null,null,null,null,null,"global",null,null,null,"global",null,null},
		    "MQTTClientRequest.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});



    protected MQTTClientRequestSchema() {
        super(FROM);
    }
    
    public static final MQTTClientRequestSchema instance = new MQTTClientRequestSchema();
    
    public static final int MSG_BROKERCONFIG_100 = 0x00000000;
    public static final int MSG_BROKERCONFIG_100_FIELD_HOST_26 = 0x01400001;
    public static final int MSG_BROKERCONFIG_100_FIELD_PORT_27 = 0x00000003;
    public static final int MSG_CONNECT_1 = 0x00000004;
    public static final int MSG_CONNECT_1_FIELD_KEEPALIVESEC_28 = 0x00000001;
    public static final int MSG_CONNECT_1_FIELD_FLAGS_29 = 0x00000002;
    public static final int MSG_CONNECT_1_FIELD_CLIENTID_30 = 0x01400003;
    public static final int MSG_CONNECT_1_FIELD_WILLTOPIC_31 = 0x01400005;
    public static final int MSG_CONNECT_1_FIELD_WILLPAYLOAD_32 = 0x01c00007;
    public static final int MSG_CONNECT_1_FIELD_USER_33 = 0x01400009;
    public static final int MSG_CONNECT_1_FIELD_PASS_34 = 0x0140000b;
    public static final int MSG_PUBLISH_3 = 0x0000000d;
    public static final int MSG_PUBLISH_3_FIELD_QOS_21 = 0x00000001;
    public static final int MSG_PUBLISH_3_FIELD_RETAIN_22 = 0x00000002;
    public static final int MSG_PUBLISH_3_FIELD_TOPIC_23 = 0x01400003;
    public static final int MSG_PUBLISH_3_FIELD_PAYLOAD_25 = 0x01c00005;
    public static final int MSG_SUBSCRIBE_8 = 0x00000013;
    public static final int MSG_SUBSCRIBE_8_FIELD_QOS_21 = 0x00000001;
    public static final int MSG_SUBSCRIBE_8_FIELD_TOPIC_23 = 0x01400002;
    public static final int MSG_UNSUBSCRIBE_10 = 0x00000017;
    public static final int MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23 = 0x01400001;


    public static void consume(Pipe<MQTTClientRequestSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_BROKERCONFIG_100:
                    consumeBrokerConfig(input);
                break;
                case MSG_CONNECT_1:
                    consumeConnect(input);
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
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeBrokerConfig(Pipe<MQTTClientRequestSchema> input) {
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_BROKERCONFIG_100_FIELD_HOST_26,new StringBuilder(PipeReader.readBytesLength(input,MSG_BROKERCONFIG_100_FIELD_HOST_26)));
        int fieldPort = PipeReader.readInt(input,MSG_BROKERCONFIG_100_FIELD_PORT_27);
    }
    public static void consumeConnect(Pipe<MQTTClientRequestSchema> input) {
        int fieldKeepAliveSec = PipeReader.readInt(input,MSG_CONNECT_1_FIELD_KEEPALIVESEC_28);
        int fieldFlags = PipeReader.readInt(input,MSG_CONNECT_1_FIELD_FLAGS_29);
        StringBuilder fieldClientId = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_CLIENTID_30,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_CLIENTID_30)));
        StringBuilder fieldWillTopic = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_WILLTOPIC_31,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_WILLTOPIC_31)));
        ByteBuffer fieldWillPayload = PipeReader.readBytes(input,MSG_CONNECT_1_FIELD_WILLPAYLOAD_32,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_WILLPAYLOAD_32)));
        StringBuilder fieldUser = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_USER_33,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_USER_33)));
        StringBuilder fieldPass = PipeReader.readUTF8(input,MSG_CONNECT_1_FIELD_PASS_34,new StringBuilder(PipeReader.readBytesLength(input,MSG_CONNECT_1_FIELD_PASS_34)));
    }
    public static void consumePublish(Pipe<MQTTClientRequestSchema> input) {
        int fieldQOS = PipeReader.readInt(input,MSG_PUBLISH_3_FIELD_QOS_21);
        int fieldRetain = PipeReader.readInt(input,MSG_PUBLISH_3_FIELD_RETAIN_22);
        StringBuilder fieldTopic = PipeReader.readUTF8(input,MSG_PUBLISH_3_FIELD_TOPIC_23,new StringBuilder(PipeReader.readBytesLength(input,MSG_PUBLISH_3_FIELD_TOPIC_23)));
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_PUBLISH_3_FIELD_PAYLOAD_25,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_PUBLISH_3_FIELD_PAYLOAD_25)));
    }
    public static void consumeSubscribe(Pipe<MQTTClientRequestSchema> input) {
        int fieldQOS = PipeReader.readInt(input,MSG_SUBSCRIBE_8_FIELD_QOS_21);
        StringBuilder fieldTopic = PipeReader.readUTF8(input,MSG_SUBSCRIBE_8_FIELD_TOPIC_23,new StringBuilder(PipeReader.readBytesLength(input,MSG_SUBSCRIBE_8_FIELD_TOPIC_23)));
    }
    public static void consumeUnSubscribe(Pipe<MQTTClientRequestSchema> input) {
        StringBuilder fieldTopic = PipeReader.readUTF8(input,MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23,new StringBuilder(PipeReader.readBytesLength(input,MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23)));
    }

    public static boolean publishBrokerConfig(Pipe<MQTTClientRequestSchema> output, CharSequence fieldHost, int fieldPort) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_BROKERCONFIG_100)) {
            PipeWriter.writeUTF8(output,MSG_BROKERCONFIG_100_FIELD_HOST_26, fieldHost);
            PipeWriter.writeInt(output,MSG_BROKERCONFIG_100_FIELD_PORT_27, fieldPort);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishConnect(Pipe<MQTTClientRequestSchema> output, int fieldKeepAliveSec, int fieldFlags, CharSequence fieldClientId, CharSequence fieldWillTopic, byte[] fieldWillPayloadBacking, int fieldWillPayloadPosition, int fieldWillPayloadLength, CharSequence fieldUser, CharSequence fieldPass) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_CONNECT_1)) {
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
    public static boolean publishPublish(Pipe<MQTTClientRequestSchema> output, int fieldQOS, int fieldRetain, CharSequence fieldTopic, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_PUBLISH_3)) {
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_QOS_21, fieldQOS);
            PipeWriter.writeInt(output,MSG_PUBLISH_3_FIELD_RETAIN_22, fieldRetain);
            PipeWriter.writeUTF8(output,MSG_PUBLISH_3_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.writeBytes(output,MSG_PUBLISH_3_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishSubscribe(Pipe<MQTTClientRequestSchema> output, int fieldQOS, CharSequence fieldTopic) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_SUBSCRIBE_8)) {
            PipeWriter.writeInt(output,MSG_SUBSCRIBE_8_FIELD_QOS_21, fieldQOS);
            PipeWriter.writeUTF8(output,MSG_SUBSCRIBE_8_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishUnSubscribe(Pipe<MQTTClientRequestSchema> output, CharSequence fieldTopic) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_UNSUBSCRIBE_10)) {
            PipeWriter.writeUTF8(output,MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23, fieldTopic);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


        
}
