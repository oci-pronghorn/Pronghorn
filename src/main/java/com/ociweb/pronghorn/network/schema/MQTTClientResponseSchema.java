package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.*;

public class MQTTClientResponseSchema extends MessageSchema<MQTTClientResponseSchema> {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400006,0x80000000,0x80000001,0x80000002,0xa8000000,0xb8000001,0xc0200006,0xc0400003,0x80000003,0xa8000002,0xc0200003,0xc0400002,0x80000004,0xc0200002},
            (short)0,
            new String[]{"Message","QOS","Retain","Dup","Topic","Payload",null,"Error","ErrorCode","ErrorText",
                    null,"CoonectionMade","sessionPresent",null},
            new long[]{3, 21, 22, 36, 23, 25, 0, 4, 41, 42, 0, 5, 51, 0},
            new String[]{"global",null,null,null,null,null,null,"global",null,null,null,"global",null,null},
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
    public static final int MSG_ERROR_4 = 0x00000007; //Group/OpenTempl/3
    public static final int MSG_ERROR_4_FIELD_ERRORCODE_41 = 0x00000001; //IntegerUnsigned/None/3
    public static final int MSG_ERROR_4_FIELD_ERRORTEXT_42 = 0x01400002; //UTF8/None/2
    public static final int MSG_COONECTIONMADE_5 = 0x0000000b; //Group/OpenTempl/2
    public static final int MSG_COONECTIONMADE_5_FIELD_SESSIONPRESENT_51 = 0x00000001; //IntegerUnsigned/None/4


    public static void consume(Pipe<MQTTClientResponseSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_MESSAGE_3:
                    consumeMessage(input);
                    break;
                case MSG_ERROR_4:
                    consumeError(input);
                    break;
                case MSG_COONECTIONMADE_5:
                    consumeCoonectionMade(input);
                    break;
                case -1:
                    //requestShutdown();
                    break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeMessage(Pipe<MQTTClientResponseSchema> input) {
        int fieldQOS = PipeReader.readInt(input,MSG_MESSAGE_3_FIELD_QOS_21);
        int fieldRetain = PipeReader.readInt(input,MSG_MESSAGE_3_FIELD_RETAIN_22);
        int fieldDup = PipeReader.readInt(input,MSG_MESSAGE_3_FIELD_DUP_36);
        StringBuilder fieldTopic = PipeReader.readUTF8(input,MSG_MESSAGE_3_FIELD_TOPIC_23,new StringBuilder(PipeReader.readBytesLength(input,MSG_MESSAGE_3_FIELD_TOPIC_23)));
        DataInputBlobReader<MQTTClientResponseSchema> fieldPayload = PipeReader.inputStream(input, MSG_MESSAGE_3_FIELD_PAYLOAD_25);
    }
    public static void consumeError(Pipe<MQTTClientResponseSchema> input) {
        int fieldErrorCode = PipeReader.readInt(input,MSG_ERROR_4_FIELD_ERRORCODE_41);
        StringBuilder fieldErrorText = PipeReader.readUTF8(input,MSG_ERROR_4_FIELD_ERRORTEXT_42,new StringBuilder(PipeReader.readBytesLength(input,MSG_ERROR_4_FIELD_ERRORTEXT_42)));
    }
    public static void consumeCoonectionMade(Pipe<MQTTClientResponseSchema> input) {
        int fieldsessionPresent = PipeReader.readInt(input,MSG_COONECTIONMADE_5_FIELD_SESSIONPRESENT_51);
    }

    public static void publishMessage(Pipe<MQTTClientResponseSchema> output, int fieldQOS, int fieldRetain, int fieldDup, CharSequence fieldTopic, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        PipeWriter.presumeWriteFragment(output, MSG_MESSAGE_3);
        PipeWriter.writeInt(output,MSG_MESSAGE_3_FIELD_QOS_21, fieldQOS);
        PipeWriter.writeInt(output,MSG_MESSAGE_3_FIELD_RETAIN_22, fieldRetain);
        PipeWriter.writeInt(output,MSG_MESSAGE_3_FIELD_DUP_36, fieldDup);
        PipeWriter.writeUTF8(output,MSG_MESSAGE_3_FIELD_TOPIC_23, fieldTopic);
        PipeWriter.writeBytes(output,MSG_MESSAGE_3_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
        PipeWriter.publishWrites(output);
    }
    public static void publishError(Pipe<MQTTClientResponseSchema> output, int fieldErrorCode, CharSequence fieldErrorText) {
        PipeWriter.presumeWriteFragment(output, MSG_ERROR_4);
        PipeWriter.writeInt(output,MSG_ERROR_4_FIELD_ERRORCODE_41, fieldErrorCode);
        PipeWriter.writeUTF8(output,MSG_ERROR_4_FIELD_ERRORTEXT_42, fieldErrorText);
        PipeWriter.publishWrites(output);
    }
    public static void publishCoonectionMade(Pipe<MQTTClientResponseSchema> output, int fieldsessionPresent) {
        PipeWriter.presumeWriteFragment(output, MSG_COONECTIONMADE_5);
        PipeWriter.writeInt(output,MSG_COONECTIONMADE_5_FIELD_SESSIONPRESENT_51, fieldsessionPresent);
        PipeWriter.publishWrites(output);
    }
}
