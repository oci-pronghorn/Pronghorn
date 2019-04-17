package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SocketDataSchema extends MessageSchema<SocketDataSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
	    new int[]{0xc0400005,0x90000000,0x90000001,0x90000002,0xb8000000,0xc0200005,0xc0400002,0x90000000,0xc0200002,0xc0400003,0x90000000,0x80000000,0xc0200003},
	    (short)0,
	    new String[]{"Data","ConnectionId","ArrivalTime","Hash","Payload",null,"Disconnect","ConnectionId",
	    null,"Upgrade","ConnectionId","NewRoute",null},
	    new long[]{210, 201, 210, 220, 204, 0, 203, 201, 0, 307, 201, 205, 0},
	    new String[]{"global",null,null,null,null,null,"global",null,null,"global",null,null,null},
	    "SocketData.xml",
	    new long[]{2, 2, 0},
	    new int[]{2, 2, 0});
	
	
	public SocketDataSchema() { 
	    super(FROM);
	}
	
	protected SocketDataSchema(FieldReferenceOffsetManager from) { 
	    super(from);
	}
	
	public static final SocketDataSchema instance = new SocketDataSchema();
	
	public static final int MSG_DATA_210 = 0x00000000; //Group/OpenTempl/5
	public static final int MSG_DATA_210_FIELD_CONNECTIONID_201 = 0x00800001; //LongUnsigned/None/0
	public static final int MSG_DATA_210_FIELD_ARRIVALTIME_210 = 0x00800003; //LongUnsigned/None/1
	public static final int MSG_DATA_210_FIELD_HASH_220 = 0x00800005; //LongUnsigned/None/2
	public static final int MSG_DATA_210_FIELD_PAYLOAD_204 = 0x01c00007; //ByteVector/None/0
	public static final int MSG_DISCONNECT_203 = 0x00000006; //Group/OpenTempl/2
	public static final int MSG_DISCONNECT_203_FIELD_CONNECTIONID_201 = 0x00800001; //LongUnsigned/None/0
	public static final int MSG_UPGRADE_307 = 0x00000009; //Group/OpenTempl/3
	public static final int MSG_UPGRADE_307_FIELD_CONNECTIONID_201 = 0x00800001; //LongUnsigned/None/0
	public static final int MSG_UPGRADE_307_FIELD_NEWROUTE_205 = 0x00000003; //IntegerUnsigned/None/0
	
	public static void consume(Pipe<SocketDataSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_DATA_210:
	                consumeData(input);
	            break;
	            case MSG_DISCONNECT_203:
	                consumeDisconnect(input);
	            break;
	            case MSG_UPGRADE_307:
	                consumeUpgrade(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}
	
	public static void consumeData(Pipe<SocketDataSchema> input) {
	    long fieldConnectionId = PipeReader.readLong(input,MSG_DATA_210_FIELD_CONNECTIONID_201);
	    long fieldArrivalTime = PipeReader.readLong(input,MSG_DATA_210_FIELD_ARRIVALTIME_210);
	    long fieldHash = PipeReader.readLong(input,MSG_DATA_210_FIELD_HASH_220);
	    DataInputBlobReader<SocketDataSchema> fieldPayload = PipeReader.inputStream(input, MSG_DATA_210_FIELD_PAYLOAD_204);
	}
	public static void consumeDisconnect(Pipe<SocketDataSchema> input) {
	    long fieldConnectionId = PipeReader.readLong(input,MSG_DISCONNECT_203_FIELD_CONNECTIONID_201);
	}
	public static void consumeUpgrade(Pipe<SocketDataSchema> input) {
	    long fieldConnectionId = PipeReader.readLong(input,MSG_UPGRADE_307_FIELD_CONNECTIONID_201);
	    int fieldNewRoute = PipeReader.readInt(input,MSG_UPGRADE_307_FIELD_NEWROUTE_205);
	}
	
	public static void publishData(Pipe<SocketDataSchema> output, long fieldConnectionId, long fieldArrivalTime, long fieldHash, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
	        PipeWriter.presumeWriteFragment(output, MSG_DATA_210);
	        PipeWriter.writeLong(output,MSG_DATA_210_FIELD_CONNECTIONID_201, fieldConnectionId);
	        PipeWriter.writeLong(output,MSG_DATA_210_FIELD_ARRIVALTIME_210, fieldArrivalTime);
	        PipeWriter.writeLong(output,MSG_DATA_210_FIELD_HASH_220, fieldHash);
	        PipeWriter.writeBytes(output,MSG_DATA_210_FIELD_PAYLOAD_204, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
	        PipeWriter.publishWrites(output);
	}
	public static void publishDisconnect(Pipe<SocketDataSchema> output, long fieldConnectionId) {
	        PipeWriter.presumeWriteFragment(output, MSG_DISCONNECT_203);
	        PipeWriter.writeLong(output,MSG_DISCONNECT_203_FIELD_CONNECTIONID_201, fieldConnectionId);
	        PipeWriter.publishWrites(output);
	}
	public static void publishUpgrade(Pipe<SocketDataSchema> output, long fieldConnectionId, int fieldNewRoute) {
	        PipeWriter.presumeWriteFragment(output, MSG_UPGRADE_307);
	        PipeWriter.writeLong(output,MSG_UPGRADE_307_FIELD_CONNECTIONID_201, fieldConnectionId);
	        PipeWriter.writeInt(output,MSG_UPGRADE_307_FIELD_NEWROUTE_205, fieldNewRoute);
	        PipeWriter.publishWrites(output);
	}


}
