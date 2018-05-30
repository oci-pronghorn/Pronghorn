package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

/**
 * Defines a typical payload for the net. This is used by HTTP, MQTT, and more. Fields include if payload
 * is encrypted, when it arrived, position, connection, a new route if required, and more.
 */
public class NetPayloadSchema extends MessageSchema<NetPayloadSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400004,0x90000000,0x90000001,0xb8000000,0xc0200004,0xc0400005,0x90000000,0x90000001,0x90000002,0xb8000001,0xc0200005,0xc0400002,0x90000000,0xc0200002,0xc0400003,0x90000000,0x80000000,0xc0200003,0xc0400002,0x88000001,0xc0200002},
		    (short)0,
		    new String[]{"Encrypted","ConnectionId","ArrivalTime","Payload",null,"Plain","ConnectionId","ArrivalTime",
		    "Position","Payload",null,"Disconnect","ConnectionId",null,"Upgrade","ConnectionId",
		    "NewRoute",null,"Begin","SequnceNo",null},
		    new long[]{200, 201, 210, 203, 0, 210, 201, 210, 206, 204, 0, 203, 201, 0, 307, 201, 205, 0, 208, 209, 0},
		    new String[]{"global",null,null,null,null,"global",null,null,null,null,null,"global",null,null,
		    "global",null,null,null,"global",null,null},
		    "NetPayload.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


    
    
    protected NetPayloadSchema() {
        super(FROM);
    }
    
    public static final NetPayloadSchema instance = new NetPayloadSchema();
    
    public static final int MSG_ENCRYPTED_200 = 0x00000000; //Group/OpenTempl/4
    public static final int MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_ENCRYPTED_200_FIELD_ARRIVALTIME_210 = 0x00800003; //LongUnsigned/None/1
    public static final int MSG_ENCRYPTED_200_FIELD_PAYLOAD_203 = 0x01c00005; //ByteVector/None/0
    public static final int MSG_PLAIN_210 = 0x00000005; //Group/OpenTempl/5
    public static final int MSG_PLAIN_210_FIELD_CONNECTIONID_201 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_PLAIN_210_FIELD_ARRIVALTIME_210 = 0x00800003; //LongUnsigned/None/1
    public static final int MSG_PLAIN_210_FIELD_POSITION_206 = 0x00800005; //LongUnsigned/None/2
    public static final int MSG_PLAIN_210_FIELD_PAYLOAD_204 = 0x01c00007; //ByteVector/None/1
    public static final int MSG_DISCONNECT_203 = 0x0000000b; //Group/OpenTempl/2
    public static final int MSG_DISCONNECT_203_FIELD_CONNECTIONID_201 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_UPGRADE_307 = 0x0000000e; //Group/OpenTempl/3
    public static final int MSG_UPGRADE_307_FIELD_CONNECTIONID_201 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_UPGRADE_307_FIELD_NEWROUTE_205 = 0x00000003; //IntegerUnsigned/None/0
    public static final int MSG_BEGIN_208 = 0x00000012; //Group/OpenTempl/2
    public static final int MSG_BEGIN_208_FIELD_SEQUNCENO_209 = 0x00400001; //IntegerSigned/None/1


    public static void consume(Pipe<NetPayloadSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_ENCRYPTED_200:
                    consumeEncrypted(input);
                break;
                case MSG_PLAIN_210:
                    consumePlain(input);
                break;
                case MSG_DISCONNECT_203:
                    consumeDisconnect(input);
                break;
                case MSG_UPGRADE_307:
                    consumeUpgrade(input);
                break;
                case MSG_BEGIN_208:
                    consumeBegin(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeEncrypted(Pipe<NetPayloadSchema> input) {
        long fieldConnectionId = PipeReader.readLong(input,MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201);
        long fieldArrivalTime = PipeReader.readLong(input,MSG_ENCRYPTED_200_FIELD_ARRIVALTIME_210);
        DataInputBlobReader<NetPayloadSchema> fieldPayload = PipeReader.inputStream(input, MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
    }
    public static void consumePlain(Pipe<NetPayloadSchema> input) {
        long fieldConnectionId = PipeReader.readLong(input,MSG_PLAIN_210_FIELD_CONNECTIONID_201);
        long fieldArrivalTime = PipeReader.readLong(input,MSG_PLAIN_210_FIELD_ARRIVALTIME_210);
        long fieldPosition = PipeReader.readLong(input,MSG_PLAIN_210_FIELD_POSITION_206);
        DataInputBlobReader<NetPayloadSchema> fieldPayload = PipeReader.inputStream(input, MSG_PLAIN_210_FIELD_PAYLOAD_204);
    }
    public static void consumeDisconnect(Pipe<NetPayloadSchema> input) {
        long fieldConnectionId = PipeReader.readLong(input,MSG_DISCONNECT_203_FIELD_CONNECTIONID_201);
    }
    public static void consumeUpgrade(Pipe<NetPayloadSchema> input) {
        long fieldConnectionId = PipeReader.readLong(input,MSG_UPGRADE_307_FIELD_CONNECTIONID_201);
        int fieldNewRoute = PipeReader.readInt(input,MSG_UPGRADE_307_FIELD_NEWROUTE_205);
    }
    public static void consumeBegin(Pipe<NetPayloadSchema> input) {
        int fieldSequnceNo = PipeReader.readInt(input,MSG_BEGIN_208_FIELD_SEQUNCENO_209);
    }

    public static void publishEncrypted(Pipe<NetPayloadSchema> output, long fieldConnectionId, long fieldArrivalTime, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
            PipeWriter.presumeWriteFragment(output, MSG_ENCRYPTED_200);
            PipeWriter.writeLong(output,MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201, fieldConnectionId);
            PipeWriter.writeLong(output,MSG_ENCRYPTED_200_FIELD_ARRIVALTIME_210, fieldArrivalTime);
            PipeWriter.writeBytes(output,MSG_ENCRYPTED_200_FIELD_PAYLOAD_203, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
    }
    public static void publishPlain(Pipe<NetPayloadSchema> output, long fieldConnectionId, long fieldArrivalTime, long fieldPosition, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
            PipeWriter.presumeWriteFragment(output, MSG_PLAIN_210);
            PipeWriter.writeLong(output,MSG_PLAIN_210_FIELD_CONNECTIONID_201, fieldConnectionId);
            PipeWriter.writeLong(output,MSG_PLAIN_210_FIELD_ARRIVALTIME_210, fieldArrivalTime);
            PipeWriter.writeLong(output,MSG_PLAIN_210_FIELD_POSITION_206, fieldPosition);
            PipeWriter.writeBytes(output,MSG_PLAIN_210_FIELD_PAYLOAD_204, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
    }
    public static void publishDisconnect(Pipe<NetPayloadSchema> output, long fieldConnectionId) {
            PipeWriter.presumeWriteFragment(output, MSG_DISCONNECT_203);
            PipeWriter.writeLong(output,MSG_DISCONNECT_203_FIELD_CONNECTIONID_201, fieldConnectionId);
            PipeWriter.publishWrites(output);
    }
    public static void publishUpgrade(Pipe<NetPayloadSchema> output, long fieldConnectionId, int fieldNewRoute) {
            PipeWriter.presumeWriteFragment(output, MSG_UPGRADE_307);
            PipeWriter.writeLong(output,MSG_UPGRADE_307_FIELD_CONNECTIONID_201, fieldConnectionId);
            PipeWriter.writeInt(output,MSG_UPGRADE_307_FIELD_NEWROUTE_205, fieldNewRoute);
            PipeWriter.publishWrites(output);
    }
    public static void publishBegin(Pipe<NetPayloadSchema> output, int fieldSequnceNo) {
            PipeWriter.presumeWriteFragment(output, MSG_BEGIN_208);
            PipeWriter.writeInt(output,MSG_BEGIN_208_FIELD_SEQUNCENO_209, fieldSequnceNo);
            PipeWriter.publishWrites(output);
    }
}
