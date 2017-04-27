package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class NetResponseSchema extends MessageSchema<NetResponseSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0xb8000000,0xc0200003,0xc0400003,0x90000000,0xb8000000,0xc0200003,0xc0400003,0xa8000001,0x80000000,0xc0200003},
		    (short)0,
		    new String[]{"Response","ConnectionId","Payload",null,"Continuation","ConnectionId","Payload", null,"Closed","Host","Port",null},
		    new long[]{101, 1, 3, 0, 102, 1, 3, 0, 10, 4, 5, 0},
		    new String[]{"global",null,null,null,"global",null,null,null,"global",null,null,null},
		    "NetResponse.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});

    
    protected NetResponseSchema() {
        super(FROM);
    }
         
    public static final NetResponseSchema instance = new NetResponseSchema();
    
    public static final int MSG_RESPONSE_101 = 0x00000000;
    public static final int MSG_RESPONSE_101_FIELD_CONNECTIONID_1 = 0x00800001;
    public static final int MSG_RESPONSE_101_FIELD_PAYLOAD_3 = 0x01c00003;
    public static final int MSG_CONTINUATION_102 = 0x00000004;
    public static final int MSG_CONTINUATION_102_FIELD_CONNECTIONID_1 = 0x00800001;
    public static final int MSG_CONTINUATION_102_FIELD_PAYLOAD_3 = 0x01c00003;
    public static final int MSG_CLOSED_10 = 0x00000008;
    public static final int MSG_CLOSED_10_FIELD_HOST_4 = 0x01400001;
    public static final int MSG_CLOSED_10_FIELD_PORT_5 = 0x00000003;


    public static void consume(Pipe<NetResponseSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_RESPONSE_101:
                    consumeResponse(input);
                break;
                case MSG_CONTINUATION_102:
                    consumeContinuation(input);
                break;
                case MSG_CLOSED_10:
                    consumeClosed(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeResponse(Pipe<NetResponseSchema> input) {
        long fieldConnectionId = PipeReader.readLong(input,MSG_RESPONSE_101_FIELD_CONNECTIONID_1);
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_RESPONSE_101_FIELD_PAYLOAD_3,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_RESPONSE_101_FIELD_PAYLOAD_3)));
    }
    public static void consumeContinuation(Pipe<NetResponseSchema> input) {
        long fieldConnectionId = PipeReader.readLong(input,MSG_CONTINUATION_102_FIELD_CONNECTIONID_1);
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_CONTINUATION_102_FIELD_PAYLOAD_3,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_CONTINUATION_102_FIELD_PAYLOAD_3)));
    }
    public static void consumeClosed(Pipe<NetResponseSchema> input) {
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_CLOSED_10_FIELD_HOST_4,new StringBuilder(PipeReader.readBytesLength(input,MSG_CLOSED_10_FIELD_HOST_4)));
        int fieldPort = PipeReader.readInt(input,MSG_CLOSED_10_FIELD_PORT_5);
    }

    public static boolean publishResponse(Pipe<NetResponseSchema> output, long fieldConnectionId, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_RESPONSE_101)) {
            PipeWriter.writeLong(output,MSG_RESPONSE_101_FIELD_CONNECTIONID_1, fieldConnectionId);
            PipeWriter.writeBytes(output,MSG_RESPONSE_101_FIELD_PAYLOAD_3, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishContinuation(Pipe<NetResponseSchema> output, long fieldConnectionId, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_CONTINUATION_102)) {
            PipeWriter.writeLong(output,MSG_CONTINUATION_102_FIELD_CONNECTIONID_1, fieldConnectionId);
            PipeWriter.writeBytes(output,MSG_CONTINUATION_102_FIELD_PAYLOAD_3, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishClosed(Pipe<NetResponseSchema> output, CharSequence fieldHost, int fieldPort) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_CLOSED_10)) {
            PipeWriter.writeUTF8(output,MSG_CLOSED_10_FIELD_HOST_4, fieldHost);
            PipeWriter.writeInt(output,MSG_CLOSED_10_FIELD_PORT_5, fieldPort);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


}
