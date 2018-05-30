package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

/**
 * Defines how a server response is formatted, including channel information, payload,
 * request , subscriptions, sequence, and more.
 */
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
    
    public static final int MSG_TOCHANNEL_100 = 0x00000000; //Group/OpenTempl/5
    public static final int MSG_TOCHANNEL_100_FIELD_CHANNELID_21 = 0x00800001; //LongUnsigned/Delta/0
    public static final int MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23 = 0x00400003; //IntegerSigned/None/0
    public static final int MSG_TOCHANNEL_100_FIELD_PAYLOAD_25 = 0x01c00004; //ByteVector/None/0
    public static final int MSG_TOCHANNEL_100_FIELD_REQUESTCONTEXT_24 = 0x00000006; //IntegerUnsigned/None/1
    public static final int MSG_TOSUBSCRIPTION_200 = 0x00000006; //Group/OpenTempl/5
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_SUBSCRIPTIONID_22 = 0x00800001; //LongUnsigned/Delta/1
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_SEQUENCENO_23 = 0x00400003; //IntegerSigned/None/0
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_PAYLOAD_25 = 0x01c00004; //ByteVector/None/0
    public static final int MSG_TOSUBSCRIPTION_200_FIELD_REQUESTCONTEXT_24 = 0x00000006; //IntegerUnsigned/None/1
    public static final int MSG_SKIP_300 = 0x0000000c; //Group/OpenTempl/2
    public static final int MSG_SKIP_300_FIELD_PAYLOAD_25 = 0x01c00001; //ByteVector/None/0


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
        DataInputBlobReader<ServerResponseSchema> fieldPayload = PipeReader.inputStream(input, MSG_TOCHANNEL_100_FIELD_PAYLOAD_25);
        int fieldRequestContext = PipeReader.readInt(input,MSG_TOCHANNEL_100_FIELD_REQUESTCONTEXT_24);
    }
    public static void consumeToSubscription(Pipe<ServerResponseSchema> input) {
        long fieldSubscriptionId = PipeReader.readLong(input,MSG_TOSUBSCRIPTION_200_FIELD_SUBSCRIPTIONID_22);
        int fieldSequenceNo = PipeReader.readInt(input,MSG_TOSUBSCRIPTION_200_FIELD_SEQUENCENO_23);
        DataInputBlobReader<ServerResponseSchema> fieldPayload = PipeReader.inputStream(input, MSG_TOSUBSCRIPTION_200_FIELD_PAYLOAD_25);
        int fieldRequestContext = PipeReader.readInt(input,MSG_TOSUBSCRIPTION_200_FIELD_REQUESTCONTEXT_24);
    }
    public static void consumeSkip(Pipe<ServerResponseSchema> input) {
        DataInputBlobReader<ServerResponseSchema> fieldPayload = PipeReader.inputStream(input, MSG_SKIP_300_FIELD_PAYLOAD_25);
    }

    public static void publishToChannel(Pipe<ServerResponseSchema> output, long fieldChannelId, int fieldSequenceNo, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength, int fieldRequestContext) {
            PipeWriter.presumeWriteFragment(output, MSG_TOCHANNEL_100);
            PipeWriter.writeLong(output,MSG_TOCHANNEL_100_FIELD_CHANNELID_21, fieldChannelId);
            PipeWriter.writeInt(output,MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23, fieldSequenceNo);
            PipeWriter.writeBytes(output,MSG_TOCHANNEL_100_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.writeInt(output,MSG_TOCHANNEL_100_FIELD_REQUESTCONTEXT_24, fieldRequestContext);
            PipeWriter.publishWrites(output);
    }
    public static void publishToSubscription(Pipe<ServerResponseSchema> output, long fieldSubscriptionId, int fieldSequenceNo, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength, int fieldRequestContext) {
            PipeWriter.presumeWriteFragment(output, MSG_TOSUBSCRIPTION_200);
            PipeWriter.writeLong(output,MSG_TOSUBSCRIPTION_200_FIELD_SUBSCRIPTIONID_22, fieldSubscriptionId);
            PipeWriter.writeInt(output,MSG_TOSUBSCRIPTION_200_FIELD_SEQUENCENO_23, fieldSequenceNo);
            PipeWriter.writeBytes(output,MSG_TOSUBSCRIPTION_200_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.writeInt(output,MSG_TOSUBSCRIPTION_200_FIELD_REQUESTCONTEXT_24, fieldRequestContext);
            PipeWriter.publishWrites(output);
    }
    public static void publishSkip(Pipe<ServerResponseSchema> output, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
            PipeWriter.presumeWriteFragment(output, MSG_SKIP_300);
            PipeWriter.writeBytes(output,MSG_SKIP_300_FIELD_PAYLOAD_25, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
    }
    
    
}
