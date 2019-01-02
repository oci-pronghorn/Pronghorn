package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ConnectionStateSchema  extends MessageSchema<ConnectionStateSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400005,0x90000000,0xb8000000,0x80000000,0x90000001,0xc0200005,0xc0400004,0x90000000,0x80000000,0x90000001,0xc0200004},
		    (short)0,
		    new String[]{"State","ArrivalTime","CustomFields","Context","BusinessStartTime",null,"StateNoEcho",
		    "ArrivalTime","Context","BusinessStartTime",null},
		    new long[]{1, 12, 14, 10, 13, 0, 2, 12, 10, 13, 0},
		    new String[]{"global",null,null,null,null,null,"global",null,null,null,null},
		    "ConnectionState.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public ConnectionStateSchema() { 
		    super(FROM);
		}

		protected ConnectionStateSchema(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final ConnectionStateSchema instance = new ConnectionStateSchema();

		public static final int MSG_STATE_1 = 0x00000000; //Group/OpenTempl/5
		public static final int MSG_STATE_1_FIELD_ARRIVALTIME_12 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_STATE_1_FIELD_CUSTOMFIELDS_14 = 0x01c00003; //ByteVector/None/0
		public static final int MSG_STATE_1_FIELD_CONTEXT_10 = 0x00000005; //IntegerUnsigned/None/0
		public static final int MSG_STATE_1_FIELD_BUSINESSSTARTTIME_13 = 0x00800006; //LongUnsigned/None/1
		public static final int MSG_STATENOECHO_2 = 0x00000006; //Group/OpenTempl/4
		public static final int MSG_STATENOECHO_2_FIELD_ARRIVALTIME_12 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_STATENOECHO_2_FIELD_CONTEXT_10 = 0x00000003; //IntegerUnsigned/None/0
		public static final int MSG_STATENOECHO_2_FIELD_BUSINESSSTARTTIME_13 = 0x00800004; //LongUnsigned/None/1

		public static void consume(Pipe<ConnectionStateSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_STATE_1:
		                consumeState(input);
		            break;
		            case MSG_STATENOECHO_2:
		                consumeStateNoEcho(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeState(Pipe<ConnectionStateSchema> input) {
		    long fieldArrivalTime = PipeReader.readLong(input,MSG_STATE_1_FIELD_ARRIVALTIME_12);
		    DataInputBlobReader<ConnectionStateSchema> fieldCustomFields = PipeReader.inputStream(input, MSG_STATE_1_FIELD_CUSTOMFIELDS_14);
		    int fieldContext = PipeReader.readInt(input,MSG_STATE_1_FIELD_CONTEXT_10);
		    long fieldBusinessStartTime = PipeReader.readLong(input,MSG_STATE_1_FIELD_BUSINESSSTARTTIME_13);
		}
		public static void consumeStateNoEcho(Pipe<ConnectionStateSchema> input) {
		    long fieldArrivalTime = PipeReader.readLong(input,MSG_STATENOECHO_2_FIELD_ARRIVALTIME_12);
		    int fieldContext = PipeReader.readInt(input,MSG_STATENOECHO_2_FIELD_CONTEXT_10);
		    long fieldBusinessStartTime = PipeReader.readLong(input,MSG_STATENOECHO_2_FIELD_BUSINESSSTARTTIME_13);
		}

		public static void publishState(Pipe<ConnectionStateSchema> output, long fieldArrivalTime, byte[] fieldCustomFieldsBacking, int fieldCustomFieldsPosition, int fieldCustomFieldsLength, int fieldContext, long fieldBusinessStartTime) {
		        PipeWriter.presumeWriteFragment(output, MSG_STATE_1);
		        PipeWriter.writeLong(output,MSG_STATE_1_FIELD_ARRIVALTIME_12, fieldArrivalTime);
		        PipeWriter.writeBytes(output,MSG_STATE_1_FIELD_CUSTOMFIELDS_14, fieldCustomFieldsBacking, fieldCustomFieldsPosition, fieldCustomFieldsLength);
		        PipeWriter.writeInt(output,MSG_STATE_1_FIELD_CONTEXT_10, fieldContext);
		        PipeWriter.writeLong(output,MSG_STATE_1_FIELD_BUSINESSSTARTTIME_13, fieldBusinessStartTime);
		        PipeWriter.publishWrites(output);
		}
		public static void publishStateNoEcho(Pipe<ConnectionStateSchema> output, long fieldArrivalTime, int fieldContext, long fieldBusinessStartTime) {
		        PipeWriter.presumeWriteFragment(output, MSG_STATENOECHO_2);
		        PipeWriter.writeLong(output,MSG_STATENOECHO_2_FIELD_ARRIVALTIME_12, fieldArrivalTime);
		        PipeWriter.writeInt(output,MSG_STATENOECHO_2_FIELD_CONTEXT_10, fieldContext);
		        PipeWriter.writeLong(output,MSG_STATENOECHO_2_FIELD_BUSINESSSTARTTIME_13, fieldBusinessStartTime);
		        PipeWriter.publishWrites(output);
		}
}
