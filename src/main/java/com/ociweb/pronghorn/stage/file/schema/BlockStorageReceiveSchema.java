package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class BlockStorageReceiveSchema extends MessageSchema<BlockStorageReceiveSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0xa8000000,0xc0200003,0xc0400002,0x90000000,0xc0200002,0xc0400003,0x90000000,0xb8000001,0xc0200003},
		    (short)0,
		    new String[]{"Error","Position","Message",null,"WriteAck","Position",null,"DataResponse","Position",
		    "Payload",null},
		    new long[]{3, 12, 10, 0, 2, 12, 0, 1, 12, 11, 0},
		    new String[]{"global",null,null,null,"global",null,null,"global",null,null,null},
		    "BlockStorageReceive.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected BlockStorageReceiveSchema() { 
		    super(FROM);
		}

		public static final BlockStorageReceiveSchema instance = new BlockStorageReceiveSchema();

		public static final int MSG_ERROR_3 = 0x00000000; //Group/OpenTempl/3
		public static final int MSG_ERROR_3_FIELD_POSITION_12 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_ERROR_3_FIELD_MESSAGE_10 = 0x01400003; //UTF8/None/0
		public static final int MSG_WRITEACK_2 = 0x00000004; //Group/OpenTempl/2
		public static final int MSG_WRITEACK_2_FIELD_POSITION_12 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_DATARESPONSE_1 = 0x00000007; //Group/OpenTempl/3
		public static final int MSG_DATARESPONSE_1_FIELD_POSITION_12 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_DATARESPONSE_1_FIELD_PAYLOAD_11 = 0x01c00003; //ByteVector/None/1


		public static void consume(Pipe<BlockStorageReceiveSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_ERROR_3:
		                consumeError(input);
		            break;
		            case MSG_WRITEACK_2:
		                consumeWriteAck(input);
		            break;
		            case MSG_DATARESPONSE_1:
		                consumeDataResponse(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeError(Pipe<BlockStorageReceiveSchema> input) {
		    long fieldPosition = PipeReader.readLong(input,MSG_ERROR_3_FIELD_POSITION_12);
		    StringBuilder fieldMessage = PipeReader.readUTF8(input,MSG_ERROR_3_FIELD_MESSAGE_10,new StringBuilder(PipeReader.readBytesLength(input,MSG_ERROR_3_FIELD_MESSAGE_10)));
		}
		public static void consumeWriteAck(Pipe<BlockStorageReceiveSchema> input) {
		    long fieldPosition = PipeReader.readLong(input,MSG_WRITEACK_2_FIELD_POSITION_12);
		}
		public static void consumeDataResponse(Pipe<BlockStorageReceiveSchema> input) {
		    long fieldPosition = PipeReader.readLong(input,MSG_DATARESPONSE_1_FIELD_POSITION_12);
		    DataInputBlobReader<BlockStorageReceiveSchema> fieldPayload = PipeReader.inputStream(input, MSG_DATARESPONSE_1_FIELD_PAYLOAD_11);
		}

		public static void publishError(Pipe<BlockStorageReceiveSchema> output, long fieldPosition, CharSequence fieldMessage) {
		        PipeWriter.presumeWriteFragment(output, MSG_ERROR_3);
		        PipeWriter.writeLong(output,MSG_ERROR_3_FIELD_POSITION_12, fieldPosition);
		        PipeWriter.writeUTF8(output,MSG_ERROR_3_FIELD_MESSAGE_10, fieldMessage);
		        PipeWriter.publishWrites(output);
		}
		public static void publishWriteAck(Pipe<BlockStorageReceiveSchema> output, long fieldPosition) {
		        PipeWriter.presumeWriteFragment(output, MSG_WRITEACK_2);
		        PipeWriter.writeLong(output,MSG_WRITEACK_2_FIELD_POSITION_12, fieldPosition);
		        PipeWriter.publishWrites(output);
		}
		public static void publishDataResponse(Pipe<BlockStorageReceiveSchema> output, long fieldPosition, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_DATARESPONSE_1);
		        PipeWriter.writeLong(output,MSG_DATARESPONSE_1_FIELD_POSITION_12, fieldPosition);
		        PipeWriter.writeBytes(output,MSG_DATARESPONSE_1_FIELD_PAYLOAD_11, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		}
}
