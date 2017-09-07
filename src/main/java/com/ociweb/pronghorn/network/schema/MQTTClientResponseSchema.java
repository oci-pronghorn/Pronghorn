package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.*;

public class MQTTClientResponseSchema extends MessageSchema<MQTTClientResponseSchema> {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400006,0x80000000,0x80000001,0x80000002,0xa8000000,0xb8000001,0xc0200006,0xc0400002,0x80000003,0xc0200002,0xc0400003,0x80000004,0x80000005,0xc0200003},
            (short)0,
            new String[]{"Message","QOS","Retain","Dup","Topic","Payload",null,"SubscriptionResult","MaxQoS",
                    null,"ConnectionAttempt","ResultCode","SessionPresent",null},
            new long[]{3, 21, 22, 36, 23, 25, 0, 4, 41, 0, 5, 51, 52, 0},
            new String[]{"global",null,null,null,null,null,null,"global",null,null,"global",null,null,null},
            "MQTTClientResponse.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});

    protected MQTTClientResponseSchema() {
        super(FROM);
    }

    public static final MQTTClientResponseSchema instance = new MQTTClientResponseSchema();

    public static final int MSG_MESSAGE_3 = 0x00000000; //Group/OpenTempl/6
    public static final int MSG_MESSAGE_3_FIELD_QOS_21 = 0x00000001; //IntegerUnsigned/None/0
    public static final int MSG_MESSAGE_3_FIELD_RETAIN_22 = 0x00000002; //IntegerUnsigned/None/1
    public static final int MSG_MESSAGE_3_FIELD_DUP_36 = 0x00000003; //IntegerUnsigned/None/2
    public static final int MSG_MESSAGE_3_FIELD_TOPIC_23 = 0x01400004; //UTF8/None/0
    public static final int MSG_MESSAGE_3_FIELD_PAYLOAD_25 = 0x01c00006; //ByteVector/None/1
    public static final int MSG_SUBSCRIPTIONRESULT_4 = 0x00000007; //Group/OpenTempl/2
    public static final int MSG_SUBSCRIPTIONRESULT_4_FIELD_MAXQOS_41 = 0x00000001; //IntegerUnsigned/None/3
    public static final int MSG_CONNECTIONATTEMPT_5 = 0x0000000a; //Group/OpenTempl/3
    public static final int MSG_CONNECTIONATTEMPT_5_FIELD_RESULTCODE_51 = 0x00000001; //IntegerUnsigned/None/4
    public static final int MSG_CONNECTIONATTEMPT_5_FIELD_SESSIONPRESENT_52 = 0x00000002; //IntegerUnsigned/None/5


    public static void consume(Pipe<MQTTClientResponseSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch (msgIdx) {
                case MSG_MESSAGE_3:
                    consumeMessage(input);
                    break;
                case MSG_SUBSCRIPTIONRESULT_4:
                    consumeSubscriptionResult(input);
                    break;
                case MSG_CONNECTIONATTEMPT_5:
                    consumeConnectionAttempt(input);
                    break;
                case -1:
                    //requestShutdown();
                    break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeMessage(Pipe<MQTTClientResponseSchema> input) {
        int fieldQOS = PipeReader.readInt(input, MSG_MESSAGE_3_FIELD_QOS_21);
        int fieldRetain = PipeReader.readInt(input, MSG_MESSAGE_3_FIELD_RETAIN_22);
        int fieldDup = PipeReader.readInt(input, MSG_MESSAGE_3_FIELD_DUP_36);
        StringBuilder fieldTopic = PipeReader.readUTF8(input, MSG_MESSAGE_3_FIELD_TOPIC_23, new StringBuilder(PipeReader.readBytesLength(input, MSG_MESSAGE_3_FIELD_TOPIC_23)));
        DataInputBlobReader<MQTTClientResponseSchema> fieldPayload = PipeReader.inputStream(input, MSG_MESSAGE_3_FIELD_PAYLOAD_25);
    }

    public static void consumeSubscriptionResult(Pipe<MQTTClientResponseSchema> input) {
        int fieldMaxQoS = PipeReader.readInt(input, MSG_SUBSCRIPTIONRESULT_4_FIELD_MAXQOS_41);
    }

    public static void consumeConnectionAttempt(Pipe<MQTTClientResponseSchema> input) {
        int fieldResultCode = PipeReader.readInt(input, MSG_CONNECTIONATTEMPT_5_FIELD_RESULTCODE_51);
        int fieldsessionPresent = PipeReader.readInt(input, MSG_CONNECTIONATTEMPT_5_FIELD_SESSIONPRESENT_52);
    }

    public static void publishMessage(Pipe<MQTTClientResponseSchema> output, int fieldQOS, int fieldRetain, int fieldDup, CharSequence fieldTopic, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        PipeWriter.presumeWriteFragment(output, MSG_MESSAGE_3);
        PipeWriter.writeInt(output, MSG_MESSAGE_3_FIELD_QOS_21, fieldQOS);
        PipeWriter.writeInt(output, MSG_MESSAGE_3_FIELD_RETAIN_22, fieldRetain);
        PipeWriter.writeInt(output, MSG_MESSAGE_3_FIELD_DUP_36, fieldDup);
        PipeWriter.writeUTF8(output, MSG_MESSAGE_3_FIELD_TOPIC_23, fieldTopic);
        PipeWriter.writeBytes(output, MSG_MESSAGE_3_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
        PipeWriter.publishWrites(output);
    }

    public static void publishSubscriptionResult(Pipe<MQTTClientResponseSchema> output, int fieldMaxQoS) {
        PipeWriter.presumeWriteFragment(output, MSG_SUBSCRIPTIONRESULT_4);
        PipeWriter.writeInt(output, MSG_SUBSCRIPTIONRESULT_4_FIELD_MAXQOS_41, fieldMaxQoS);
        PipeWriter.publishWrites(output);
    }

    public static void publishConnectionAttempt(Pipe<MQTTClientResponseSchema> output, int fieldResultCode, int fieldsessionPresent) {
        PipeWriter.presumeWriteFragment(output, MSG_CONNECTIONATTEMPT_5);
        PipeWriter.writeInt(output, MSG_CONNECTIONATTEMPT_5_FIELD_RESULTCODE_51, fieldResultCode);
        PipeWriter.writeInt(output, MSG_CONNECTIONATTEMPT_5_FIELD_SESSIONPRESENT_52, fieldsessionPresent);
        PipeWriter.publishWrites(output);
    }
}