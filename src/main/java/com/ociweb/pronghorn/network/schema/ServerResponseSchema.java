package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ServerResponseSchema extends MessageSchema<ServerResponseSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400005,0x90800000,0x88000000,0xb8000000,0x80000001,0xc0200005,0xc0400005,0x90800001,0x88000000,0xb8000000,0x80000001,0xc0200005,0xc0400002,0xb8000000,0xc0200002},
		    (short)0,
		    new String[]{"ToChannel","ChannelId","SequenceNo","Payload","RequestContext",null,"ToSubscription","SubscriptionId","SequenceNo","Payload","RequestContext",null,"Skip","Payload",null},
		    new long[]{100, 21, 23, 25, 24, 0, 200, 22, 23, 25, 24, 0, 300, 25, 0},
		    new String[]{"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null},
		    "serverResponse.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    

 
    
    private ServerResponseSchema() {
        super(FROM);
    }

    public static final ServerResponseSchema instance = new ServerResponseSchema();
    
    public static final int MSG_TOCHANNEL_100 = 0x00000000;
    public static final int MSG_TOCHANNEL_100_FIELD_CHANNELID_21 = 0x00800001;
    public static final int MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23 = 0x00400003;
    public static final int MSG_TOCHANNEL_100_FIELD_PAYLOAD_25 = 0x01c00004;
    public static final int MSG_TOCHANNEL_100_FIELD_REQUESTCONTEXT_24 = 0x00000006;
    public static final int MSG_TOSUBSCRIPTION_200 = 0x00000006;
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_SUBSCRIPTIONID_22 = 0x00800001;
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_SEQUENCENO_23 = 0x00400003;
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_PAYLOAD_25 = 0x01c00004;
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_REQUESTCONTEXT_24 = 0x00000006;
    public static final int MSG_SKIP_300 = 0x0000000c;
    public static final int MSG_SKIP_300_FIELD_PAYLOAD_25 = 0x01c00001;


    public static void consume(Pipe<ServerResponseSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_TOCHANNEL_100:
                    consumeToChannel(input);
                break;
                case MSG_TOSUBSCRIPTION_200:
                    consumeToSubscription(input);
                break;
                case MSG_SKIP_300:
                    consumeSkip(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeToChannel(Pipe<ServerResponseSchema> input) {
        long fieldChannelId = PipeReader.readLong(input,MSG_TOCHANNEL_100_FIELD_CHANNELID_21);
        int fieldSequenceNo = PipeReader.readInt(input,MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23);
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_TOCHANNEL_100_FIELD_PAYLOAD_25,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_TOCHANNEL_100_FIELD_PAYLOAD_25)));
        int fieldRequestContext = PipeReader.readInt(input,MSG_TOCHANNEL_100_FIELD_REQUESTCONTEXT_24);
    }
    public static void consumeToSubscription(Pipe<ServerResponseSchema> input) {
        long fieldSubscriptionId = PipeReader.readLong(input,MSG_TOSUBSCRIPTION_200_FIELD_SUBSCRIPTIONID_22);
        int fieldSequenceNo = PipeReader.readInt(input,MSG_TOSUBSCRIPTION_200_FIELD_SEQUENCENO_23);
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_TOSUBSCRIPTION_200_FIELD_PAYLOAD_25,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_TOSUBSCRIPTION_200_FIELD_PAYLOAD_25)));
        int fieldRequestContext = PipeReader.readInt(input,MSG_TOSUBSCRIPTION_200_FIELD_REQUESTCONTEXT_24);
    }
    public static void consumeSkip(Pipe<ServerResponseSchema> input) {
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_SKIP_300_FIELD_PAYLOAD_25,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_SKIP_300_FIELD_PAYLOAD_25)));
    }

    public static boolean publishToChannel(Pipe<ServerResponseSchema> output, long fieldChannelId, int fieldSequenceNo, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength, int fieldRequestContext) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_TOCHANNEL_100)) {
            PipeWriter.writeLong(output,MSG_TOCHANNEL_100_FIELD_CHANNELID_21, fieldChannelId);
            PipeWriter.writeInt(output,MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23, fieldSequenceNo);
            PipeWriter.writeBytes(output,MSG_TOCHANNEL_100_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.writeInt(output,MSG_TOCHANNEL_100_FIELD_REQUESTCONTEXT_24, fieldRequestContext);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishToSubscription(Pipe<ServerResponseSchema> output, long fieldSubscriptionId, int fieldSequenceNo, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength, int fieldRequestContext) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_TOSUBSCRIPTION_200)) {
            PipeWriter.writeLong(output,MSG_TOSUBSCRIPTION_200_FIELD_SUBSCRIPTIONID_22, fieldSubscriptionId);
            PipeWriter.writeInt(output,MSG_TOSUBSCRIPTION_200_FIELD_SEQUENCENO_23, fieldSequenceNo);
            PipeWriter.writeBytes(output,MSG_TOSUBSCRIPTION_200_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.writeInt(output,MSG_TOSUBSCRIPTION_200_FIELD_REQUESTCONTEXT_24, fieldRequestContext);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishSkip(Pipe<ServerResponseSchema> output, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_SKIP_300)) {
            PipeWriter.writeBytes(output,MSG_SKIP_300_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


    
    
}
